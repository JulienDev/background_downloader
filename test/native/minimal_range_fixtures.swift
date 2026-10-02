import Foundation
import os.log

// Test-only bridge for the real Task, Auth and RangeDownloader sources.
let log = OSLog(subsystem: "MinimalRangeFixtures", category: "Tests")
let chunkGroup = "chunk"
func isNotFinalState(status: TaskStatus) -> Bool { [.enqueued, .running, .paused, .waitingToRetry].contains(status) }
func isFinalState(status: TaskStatus) -> Bool { !isNotFinalState(status: status) }

let root = URL(fileURLWithPath: "/private/tmp/minimal-range-\(UUID().uuidString)")

final class FakeTransport: RangeTransport {
    var started = [(piece: Task, earliestBegin: Date?)]()
    var canceled = [String]()
    var live = Set<String>()
    var wifiViolations = Set<String>()
    var refuse = false

    func start(piece: Task, earliestBegin: Date?) -> Bool {
        if refuse { return false }
        started.append((piece, earliestBegin))
        live.insert(piece.taskId)
        return true
    }
    func cancel(pieceTaskId: String) {
        canceled.append(pieceTaskId)
        live.remove(pieceTaskId)
    }
    func liveTaskIds(_ completion: @escaping (Set<String>) -> Void) { completion(live) }
    func violatesWiFiPolicy(pieceTaskId: String) -> Bool { wifiViolations.contains(pieceTaskId) }
    var current: Task { started.last!.piece }
}

final class FakeEvents: RangeEvents {
    var statuses = [(String, TaskStatus, TaskException?)]()
    var progress = [Int64]()
    var resumableIds = [String]()
    var notices = [RangeNotice]()
    var finishedIds = [String]()
    var publishedFiles = [URL]()
    var destinationDirectory = root.appendingPathComponent("downloads")
    /// When set, every event waits on another thread that needs the downloader,
    /// as the plugin's main-thread channel does: delivery under lock would hang.
    weak var probe: RangeDownloader?
    var probes = 0

    func requireUnlocked() {
        guard let probe = probe else { return }
        let done = DispatchSemaphore(value: 0)
        DispatchQueue.global().async {
            _ = probe.activeParents()
            done.signal()
        }
        precondition(done.wait(timeout: .now() + 2) == .success, "range event delivered while holding the lock")
        probes += 1
    }

    func status(_ parent: Task, _ status: TaskStatus, exception: TaskException?, responseStatusCode: Int?) {
        requireUnlocked()
        statuses.append((parent.taskId, status, exception))
    }
    func progress(_ parent: Task, done: Int64, total: Int64) {
        requireUnlocked()
        progress.append(done)
    }
    func resumable(_ parent: Task) { resumableIds.append(parent.taskId) }
    func notice(_ parent: Task, _ notice: RangeNotice, notificationConfig: String?) { notices.append(notice) }
    func finished(_ parent: Task) { finishedIds.append(parent.taskId) }
    func destination(for parent: Task) -> URL? { destinationDirectory.appendingPathComponent(parent.filename) }
    func published(_ file: URL) { publishedFiles.append(file) }
    func last(_ id: String) -> TaskStatus? { statuses.last(where: { $0.0 == id })?.1 }
}

@main
enum MinimalRangeFixtures {
    static var assertions = 0
    static var clock = Date(timeIntervalSince1970: 1_000_000)

    static func check(_ condition: Bool, _ message: String) {
        assertions += 1
        precondition(condition, message)
    }

    static func source(_ size: Int) -> Data { Data((0..<size).map { UInt8(($0 * 7 + 3) % 251) }) }

    static func parent(_ id: String, size: Int64, piece: Int64 = 1 << 20, retries: Int = 3) -> Task {
        Task(taskId: id, url: "http://203.0.113.7:8000/movie/\(id).mkv", urls: ["http://203.0.113.7:8000/movie/\(id).mkv"],
             filename: "\(id).mkv", headers: ["Known-Content-Length": String(size), "Authorization": "fixture-auth",
                                              "Accept-Encoding": "gzip"],
             chunks: 1, baseDirectory: 0, group: "serie", updates: 3, requiresWiFi: true,
             retries: retries, retriesRemaining: 0, allowPause: true,
             metaData: "{\"rangeChunks\":true,\"rangePieceBytes\":\(piece)}", taskType: "ParallelDownloadTask")
    }

    final class Deferred {
        var work = [() -> Void]()
        func run() {
            let pending = work
            work.removeAll()
            pending.forEach { $0() }
        }
    }

    struct Harness {
        let store: RangeStore
        let downloader: RangeDownloader
        let transport = FakeTransport()
        let events = FakeEvents()
        /// Foreground checks wait here until the test runs them.
        let promotions = Deferred()

        init(directory: URL) {
            store = RangeStore(directory: directory)
            downloader = RangeDownloader(store: store)
            downloader.transport = transport
            downloader.events = events
            downloader.now = { MinimalRangeFixtures.clock }
            downloader.restoreDelay = { $0() }
            let promotions = promotions
            downloader.promoteDelay = { promotions.work.append($0) }
        }
    }

    static func response(_ piece: Task, status: Int = 206, range: String? = nil, length: String? = nil) -> HTTPURLResponse {
        let metadata = rangeChunkMetadata(piece)!
        let from = Int64(metadata["from"]!)!, to = Int64(metadata["to"]!)!, total = metadata["rangeTotal"]!
        var headers = ["Accept-Ranges": "0-\(total)"]
        headers["Content-Range"] = range ?? "bytes \(from)-\(to)/\(total)"
        headers["Content-Length"] = length ?? String(to - from + 1)
        return HTTPURLResponse(url: URL(string: piece.url)!, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers)!
    }

    /// Delivers the piece bytes from [data] (or [body]) through the session callbacks.
    @discardableResult
    static func deliver(_ h: Harness, _ data: Data, status: Int = 206, body: Data? = nil, range: String? = nil) -> Bool {
        let piece = h.transport.current
        let metadata = rangeChunkMetadata(piece)!
        let from = Int(metadata["from"]!)!, to = Int(metadata["to"]!)!
        let payload = body ?? data.subdata(in: from..<(to + 1))
        let location = root.appendingPathComponent("location-\(UUID().uuidString)")
        try! payload.write(to: location)
        let response = response(piece, status: status, range: range, length: String(payload.count))
        _ = h.downloader.handleProgress(piece: piece, response: response, totalBytesWritten: Int64(payload.count / 2))
        let handled = h.downloader.handleFinished(piece: piece, response: response, location: location)
        _ = h.downloader.handleCompleted(piece: piece, error: nil)
        try? FileManager.default.removeItem(at: location)
        return handled
    }

    static func output(_ h: Harness, _ id: String) -> Data? {
        try? Data(contentsOf: h.events.destinationDirectory.appendingPathComponent("\(id).mkv"))
    }

    static func main() {
        try! FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let mib = 1 << 20

        // Markers and planning.
        let marked = parent("plan", size: 5 * Int64(mib) / 2)
        check(provenRangeLength(marked) == Int64(5 * mib / 2), "proven length comes from Known-Content-Length")
        check(provenRangeLength(marked.copyWith(metaData: "{}")) == nil, "unmarked parent is not a range download")
        check(rangePieceBytes(marked) == Int64(mib), "piece size from metadata")
        check(rangePieceBytes(marked.copyWith(metaData: "{\"rangeChunks\":true,\"rangePieceBytes\":10}")) == minimumRangePieceBytes,
              "piece size has a 1 MiB floor")
        check(rangePieceBytes(marked.copyWith(metaData: "{\"rangeChunks\":true}")) == defaultRangePieceBytes, "default piece size")

        // A state saved before newer fields existed still decodes.
        do {
            var legacy = try! JSONSerialization.jsonObject(with: JSONEncoder().encode(
                RangeDownloadState(parent: marked, notificationConfig: nil, total: 10))) as! [String: Any]
            legacy.removeValue(forKey: "failed")
            legacy["committed"] = 4
            let decoded = try? JSONDecoder().decode(RangeDownloadState.self,
                                                    from: JSONSerialization.data(withJSONObject: legacy))
            check(decoded?.failed == false && decoded?.committed == 4, "legacy state decodes with defaults")
        }

        // Happy path: sequential pieces, exact bytes, monotonic progress, cleanup.
        do {
            let h = Harness(directory: root.appendingPathComponent("happy"))
            let size = 5 * mib / 2
            let data = source(size)
            let task = parent("happy", size: Int64(size))
            h.events.probe = h.downloader
            check(h.downloader.start(parent: task, notificationConfig: "{}"), "range download starts")
            let first = h.transport.current
            check(first.url == task.url && first.headers["Authorization"] == "fixture-auth", "piece keeps origin and auth")
            check(first.headers["Range"] == "bytes=0-\(mib - 1)" && first.headers["Accept-Encoding"] == "identity", "first piece range")
            check(first.headers["Known-Content-Length"] == nil && first.group == chunkGroup && first.requiresWiFi, "piece headers and WiFi")
            check(rangeChunkMetadata(first)?["parentTaskId"] == "happy", "piece names its parent")
            check(h.transport.started.first?.earliestBegin == nil, "first piece starts immediately")
            for _ in 0..<3 { check(deliver(h, data), "piece handled") }
            check(h.transport.started.count == 3, "three sequential pieces")
            check(h.transport.started[2].piece.headers["Range"] == "bytes=\(2 * mib)-\(size - 1)", "last piece is short")
            check(output(h, "happy") == data, "published bytes are exact")
            check(h.events.last("happy") == .complete && h.events.finishedIds == ["happy"], "complete and released")
            check(h.events.statuses.filter { $0.1 == .running }.count == 1, "running reported once")
            check(h.events.progress == h.events.progress.sorted(), "progress never decreases")
            check(h.events.notices.first == .running && h.events.notices.last == .complete, "notifications follow status")
            check(!h.downloader.owns(parentTaskId: "happy") && h.store.loadAll().isEmpty, "state removed")
            check(!FileManager.default.fileExists(atPath: h.store.partialURL("happy").path), "partial moved away")
            check(h.events.publishedFiles.count == 1, "published file reported for backup policy")
            check(h.events.probes > 5, "events delivered outside the lock")
        }

        // 406 with a stale token: retry the same range after backoff, keep committed bytes.
        do {
            let h = Harness(directory: root.appendingPathComponent("retry"))
            let size = 3 * mib
            let data = source(size)
            h.downloader.start(parent: parent("retry", size: Int64(size)), notificationConfig: nil)
            deliver(h, data)
            let before = h.downloader.state("retry")!
            check(before.committed == Int64(mib), "first piece committed")
            check(deliver(h, data, status: 406, body: Data("Signed token expired".utf8)), "406 handled")
            let retry = h.transport.started.last!
            check(retry.piece.headers["Range"] == "bytes=\(mib)-\(2 * mib - 1)", "same range retried")
            check(retry.piece.taskId != h.transport.started[1].piece.taskId, "new attempt id")
            check(retry.earliestBegin == clock.addingTimeInterval(2), "first retry waits 2 s in the session")
            check(h.downloader.state("retry")!.committed == Int64(mib), "committed bytes kept")
            check(h.events.last("retry") == .running, "no failure surfaced while retrying")
            deliver(h, data, range: "bytes \(mib)-\(2 * mib - 1)/999")
            check(h.transport.started.last!.earliestBegin == clock.addingTimeInterval(4), "second retry waits 4 s")
            deliver(h, data)
            check(h.downloader.state("retry")!.retriesRemaining == 3, "budget refilled after a committed piece")
            deliver(h, data)
            check(output(h, "retry") == data, "bytes exact after retries")
        }

        // A 200 that ignores Range is rejected on its first data, before any commit.
        do {
            let h = Harness(directory: root.appendingPathComponent("ignored"))
            let data = source(2 * mib)
            h.downloader.start(parent: parent("ignored", size: Int64(2 * mib)), notificationConfig: nil)
            let piece = h.transport.current
            let full = HTTPURLResponse(url: URL(string: piece.url)!, statusCode: 200, httpVersion: "HTTP/1.1",
                                       headerFields: ["Content-Length": String(2 * mib)])!
            check(h.downloader.handleProgress(piece: piece, response: full, totalBytesWritten: 10), "200 handled")
            check(h.transport.canceled == [piece.taskId], "ignored-range transfer canceled")
            check(h.downloader.handleCompleted(piece: piece, error: URLError(.cancelled)), "own cancellation absorbed")
            check(h.transport.started.count == 2 && h.events.last("ignored") != .failed, "retried without failing")
            deliver(h, data)
            deliver(h, data)
            check(output(h, "ignored") == data, "bytes exact after ignored range")
        }

        // A piece the session resumed itself is accepted only when its file is whole.
        do {
            let h = Harness(directory: root.appendingPathComponent("resumed"))
            let data = source(2 * mib)
            h.downloader.start(parent: parent("resumed", size: Int64(2 * mib)), notificationConfig: nil)
            func finishResumed(start: Int, file: Data) {
                let piece = h.transport.current
                let to = Int(rangeChunkMetadata(piece)!["to"]!)!
                let resumed = response(piece, range: "bytes \(start)-\(to)/\(2 * mib)", length: String(to - start + 1))
                let location = root.appendingPathComponent("resumed-\(UUID().uuidString)")
                try! file.write(to: location)
                _ = h.downloader.handleProgress(piece: piece, response: resumed, totalBytesWritten: Int64(file.count))
                _ = h.downloader.handleFinished(piece: piece, response: resumed, location: location)
                _ = h.downloader.handleCompleted(piece: piece, error: nil)
            }
            finishResumed(start: 1000, file: data.subdata(in: 1000..<mib))
            check(h.downloader.state("resumed")!.committed == 0 && h.transport.started.count == 2,
                  "resumed bytes without their prefix are rejected")
            finishResumed(start: 1000, file: data.subdata(in: 0..<mib))
            check(h.downloader.state("resumed")!.committed == Int64(mib), "whole resumed piece committed")
            finishResumed(start: mib + 5, file: data.subdata(in: mib..<(2 * mib)))
            check(output(h, "resumed") == data, "bytes exact after session resumes")
        }

        // Each consecutive failure halves the piece; a committed piece restores it.
        do {
            let h = Harness(directory: root.appendingPathComponent("shrink"))
            let data = source(20 * mib)
            h.downloader.start(parent: parent("shrink", size: Int64(20 * mib), piece: Int64(8 * mib)), notificationConfig: nil)
            check(h.transport.current.headers["Range"] == "bytes=0-\(8 * mib - 1)", "normal piece")
            _ = h.downloader.handleCompleted(piece: h.transport.current, error: URLError(.networkConnectionLost))
            check(h.transport.current.headers["Range"] == "bytes=0-\(4 * mib - 1)", "halved after one failure")
            _ = h.downloader.handleCompleted(piece: h.transport.current, error: URLError(.networkConnectionLost))
            check(h.transport.current.headers["Range"] == "bytes=0-\(2 * mib - 1)", "halved again")
            deliver(h, data)
            check(h.transport.current.headers["Range"] == "bytes=\(2 * mib)-\(10 * mib - 1)", "normal size after a commit")
            while h.downloader.owns(parentTaskId: "shrink") { deliver(h, data) }
            check(output(h, "shrink") == data, "bytes exact with shrinking pieces")
        }

        // An append from a canceled and restarted download is not committed.
        do {
            let h = Harness(directory: root.appendingPathComponent("generation"))
            let task = parent("generation", size: Int64(2 * mib))
            h.downloader.start(parent: task, notificationConfig: nil)
            let first = h.downloader.state("generation")!.generation
            _ = h.downloader.cancel(parentTaskId: "generation")
            h.downloader.start(parent: task, notificationConfig: nil)
            check(h.downloader.state("generation")!.generation != first, "restart gets a new generation")
        }

        // Exhausted retries fail the download but keep its bytes for a retry.
        do {
            let h = Harness(directory: root.appendingPathComponent("exhaust"))
            let data = source(3 * mib)
            let task = parent("exhaust", size: Int64(3 * mib), retries: 2)
            h.downloader.start(parent: task, notificationConfig: nil)
            deliver(h, data)
            for _ in 0..<3 {
                _ = h.downloader.handleCompleted(piece: h.transport.current, error: URLError(.networkConnectionLost))
            }
            check(h.transport.started.count == 4, "initial attempt plus two retries")
            check(h.events.last("exhaust") == .failed, "fails after the budget")
            check(h.events.statuses.last?.2?.type == .connection, "connection failure type")
            check(h.events.resumableIds == ["exhaust"] && h.events.finishedIds == ["exhaust"], "resumable and queue slot released")
            check(h.downloader.state("exhaust")!.failed && h.store.partialSize("exhaust") == Int64(mib), "committed bytes kept")
            check(h.downloader.activeParents().isEmpty, "failed parent is not active")
            h.downloader.restoreAfterLaunch()
            check(h.transport.started.count == 4, "failed download is not restarted by itself")
            let progressBefore = h.events.progress.count
            check(h.downloader.start(parent: task, notificationConfig: nil), "user retry accepted")
            check(h.events.progress.count == progressBefore + 1 && h.events.progress.last == Int64(mib),
                  "retained progress shown immediately")
            check(h.transport.current.headers["Range"] == "bytes=\(mib)-\(2 * mib - 1)", "retry continues at committed offset")
            check(h.downloader.state("exhaust")!.retriesRemaining == 2, "retry gets a full budget")
            deliver(h, data)
            deliver(h, data)
            check(output(h, "exhaust") == data, "bytes exact after a failed and retried download")
        }

        // 404 is not retried automatically, but keeps committed bytes.
        do {
            let h = Harness(directory: root.appendingPathComponent("missing"))
            let data = source(2 * mib)
            h.downloader.start(parent: parent("missing", size: Int64(2 * mib)), notificationConfig: nil)
            deliver(h, data)
            deliver(h, data, status: 404, body: Data("missing".utf8))
            check(h.events.last("missing") == .notFound && h.transport.started.count == 2, "404 is not retried")
            check(h.store.partialSize("missing") == Int64(mib) && h.downloader.owns(parentTaskId: "missing"), "404 keeps bytes")
            check(h.downloader.cancel(parentTaskId: "missing"), "failed download can be canceled")
            check(!FileManager.default.fileExists(atPath: h.store.partialURL("missing").path), "cancel deletes kept bytes")
        }

        // Stale attempts and orphans never touch the download.
        do {
            let h = Harness(directory: root.appendingPathComponent("stale"))
            let data = source(2 * mib)
            h.downloader.start(parent: parent("stale", size: Int64(2 * mib)), notificationConfig: nil)
            let old = h.transport.current
            _ = h.downloader.handleCompleted(piece: old, error: URLError(.timedOut))
            let location = root.appendingPathComponent("stale-location")
            try! data.subdata(in: 0..<mib).write(to: location)
            check(h.downloader.handleFinished(piece: old, response: response(old), location: location), "stale piece handled")
            check(h.downloader.state("stale")!.committed == 0, "stale completion ignored")
            let orphan = rangePieceTask(RangeDownloadState(parent: parent("ghost", size: 10), notificationConfig: nil, total: 10),
                                        from: 0, to: 9)
            check(h.downloader.handleProgress(piece: orphan, response: nil, totalBytesWritten: 1), "orphan handled")
            check(h.transport.canceled.contains(orphan.taskId), "orphan transfer canceled")
            check(!h.downloader.handleProgress(piece: parent("plain", size: 10).copyWith(group: "serie", taskType: "DownloadTask"),
                                               response: nil, totalBytesWritten: 1), "ordinary tasks are not intercepted")
        }

        // Pause drops only the piece in flight; resume continues at the committed offset.
        do {
            let h = Harness(directory: root.appendingPathComponent("pause"))
            let data = source(3 * mib)
            let task = parent("pause", size: Int64(3 * mib))
            h.downloader.start(parent: task, notificationConfig: nil)
            deliver(h, data)
            let inFlight = h.transport.current
            _ = h.downloader.handleProgress(piece: inFlight, response: response(inFlight), totalBytesWritten: 1000)
            check(h.downloader.pause(parentTaskId: "pause"), "pause accepted")
            check(h.transport.canceled.contains(inFlight.taskId), "piece in flight canceled")
            check(h.events.resumableIds == ["pause"] && h.events.last("pause") == .paused, "paused with resume marker")
            check(h.events.finishedIds == ["pause"], "paused parent releases its queue slot")
            check(!h.downloader.pause(parentTaskId: "pause"), "second pause refused")
            check(h.downloader.activeParents().isEmpty, "paused parent is not active")
            _ = h.downloader.handleCompleted(piece: inFlight, error: URLError(.cancelled))
            check(h.downloader.start(parent: task, notificationConfig: nil), "resume accepted")
            check(h.events.progress.last == h.downloader.state("pause")!.displayed, "resume shows retained progress")
            check(h.transport.current.headers["Range"] == "bytes=\(mib)-\(2 * mib - 1)", "resume at committed offset")
            let runningBefore = h.events.statuses.filter { $0.1 == .running }.count
            deliver(h, data)
            check(h.events.statuses.filter { $0.1 == .running }.count == runningBefore + 1, "running reported again after resume")
            deliver(h, data)
            check(output(h, "pause") == data, "bytes exact after pause")
        }

        // Cancel removes everything.
        do {
            let h = Harness(directory: root.appendingPathComponent("cancel"))
            let data = source(2 * mib)
            h.downloader.start(parent: parent("cancel", size: Int64(2 * mib)), notificationConfig: nil)
            deliver(h, data)
            check(h.downloader.cancel(parentTaskId: "cancel"), "cancel accepted")
            check(h.events.last("cancel") == .canceled && h.events.notices.last == .canceled, "canceled reported")
            check(h.store.loadAll().isEmpty && !FileManager.default.fileExists(atPath: h.store.partialURL("cancel").path),
                  "cancel removes state and partial")
            check(output(h, "cancel") == nil, "nothing published")
        }

        // Process termination: a new controller restores state and restarts a lost piece.
        do {
            let directory = root.appendingPathComponent("restore")
            let data = source(3 * mib)
            let task = parent("restore", size: Int64(3 * mib))
            let first = Harness(directory: directory)
            first.downloader.start(parent: task, notificationConfig: "{\"x\":1}")
            deliver(first, data)
            let lost = first.transport.current
            // Simulate an append interrupted by termination: extra bytes beyond committed.
            let handle = try! FileHandle(forWritingTo: first.store.partialURL("restore"))
            try! handle.seekToEnd()
            try! handle.write(contentsOf: Data(repeating: 9, count: 123))
            try! handle.close()

            let second = Harness(directory: directory)
            check(second.downloader.owns(parentTaskId: "restore"), "state restored")
            check(second.store.partialSize("restore") == Int64(mib), "interrupted append truncated")
            check(second.downloader.activeParents().map { $0.taskId } == ["restore"], "parent reported to Dart reconciliation")
            second.transport.live = [lost.taskId]
            second.downloader.restoreAfterLaunch()
            check(second.transport.started.isEmpty, "live piece is left running")
            second.transport.live = []
            second.downloader.restoreAfterLaunch()
            check(second.transport.current.headers["Range"] == "bytes=\(mib)-\(2 * mib - 1)", "lost piece restarted at offset")
            check(second.transport.canceled.contains(lost.taskId), "lost attempt abandoned before restarting")
            check(second.downloader.state("restore")!.notificationConfig == "{\"x\":1}", "notification config persisted")
            check(second.downloader.start(parent: task, notificationConfig: nil), "rescheduled enqueue continues")
            check(second.transport.started.count == 1, "continuing does not duplicate a live piece")
            deliver(second, data)
            deliver(second, data)
            check(output(second, "restore") == data, "bytes exact after restore")
        }

        // A shorter partial (lost write) falls back to the last whole piece.
        do {
            let directory = root.appendingPathComponent("short")
            let data = source(3 * mib)
            let first = Harness(directory: directory)
            first.downloader.start(parent: parent("short", size: Int64(3 * mib)), notificationConfig: nil)
            deliver(first, data)
            deliver(first, data)
            let handle = try! FileHandle(forWritingTo: first.store.partialURL("short"))
            try! handle.truncate(atOffset: UInt64(mib + 5))
            try! handle.close()
            let second = Harness(directory: directory)
            check(second.downloader.state("short")!.committed == Int64(mib), "committed aligned to a whole piece")
            second.downloader.restoreAfterLaunch()
            deliver(second, data)
            deliver(second, data)
            check(output(second, "short") == data, "bytes exact after realignment")
        }

        // In the background a piece runs to the end of the file; after a failure
        // pieces stay bounded. An idle background piece is recreated once the
        // app is active; one that delivers data is left alone.
        do {
            let h = Harness(directory: root.appendingPathComponent("promote"))
            let data = source(3 * mib)
            var active = false
            h.downloader.isAppActive = { active }
            h.downloader.start(parent: parent("promote", size: Int64(3 * mib)), notificationConfig: nil)
            let open = h.transport.current
            check(open.headers["Range"] == "bytes=0-\(3 * mib - 1)", "background piece covers up to four pieces")
            check(h.downloader.state("promote")!.pieceStartedInBackground, "background piece marked")
            _ = h.downloader.handleCompleted(piece: open, error: URLError(.timedOut))
            check(h.transport.current.headers["Range"] == "bytes=0-\(mib - 1)", "after a failure, background pieces are bounded")
            let waiting = h.transport.current
            active = true
            h.downloader.promoteOnForeground()
            check(h.transport.started.count == 2, "promotion waits before judging")
            h.promotions.run()
            check(h.transport.canceled.contains(waiting.taskId) && h.transport.started.count == 3, "idle piece recreated")
            check(!h.downloader.state("promote")!.pieceStartedInBackground, "recreated piece is foreground")
            h.downloader.promoteOnForeground()
            h.promotions.run()
            check(h.transport.started.count == 3, "foreground piece left alone")
            active = false
            deliver(h, data)
            let receiving = h.transport.current
            check(receiving.headers["Range"] == "bytes=\(mib)-\(3 * mib - 1)", "next background piece is large again")
            active = true
            h.downloader.promoteOnForeground()
            clock = clock.addingTimeInterval(1)
            _ = h.downloader.handleProgress(piece: receiving, response: response(receiving), totalBytesWritten: 4096)
            h.promotions.run()
            check(h.transport.started.count == 4, "a background piece receiving data is not restarted")
            deliver(h, data)
            check(output(h, "promote") == data, "bytes exact across background and foreground pieces")
        }

        // Background pieces are larger but bounded; the first piece is moved.
        do {
            let h = Harness(directory: root.appendingPathComponent("bounded"))
            let data = source(10 * mib)
            h.downloader.isAppActive = { false }
            h.downloader.start(parent: parent("bounded", size: Int64(10 * mib)), notificationConfig: nil)
            let piece = h.transport.current
            check(piece.headers["Range"] == "bytes=0-\(4 * mib - 1)", "background piece is four pieces, not the whole file")
            let location = root.appendingPathComponent("bounded-location")
            try! data.subdata(in: 0..<(4 * mib)).write(to: location)
            _ = h.downloader.handleFinished(piece: piece, response: response(piece), location: location)
            check(!FileManager.default.fileExists(atPath: location.path), "first piece moved into the partial file")
            check(h.store.partialSize("bounded") == Int64(4 * mib), "moved piece committed")
            check(h.transport.current.headers["Range"] == "bytes=\(4 * mib)-\(8 * mib - 1)", "next background piece bounded")
            h.downloader.isAppActive = { true }
            deliver(h, data)
            deliver(h, data)
            deliver(h, data)
            check(output(h, "bounded") == data, "bytes exact with background pieces")
        }

        // Termination during publication completes or publishes again.
        do {
            let directory = root.appendingPathComponent("publish")
            let data = source(mib)
            let task = parent("publish", size: Int64(mib))
            let first = Harness(directory: directory)
            first.downloader.start(parent: task, notificationConfig: nil)
            var state = first.downloader.state("publish")!
            state.publishing = true
            state.committed = Int64(mib)
            first.store.save(state)
            let destination = first.events.destinationDirectory.appendingPathComponent("publish.mkv")
            try! FileManager.default.createDirectory(at: first.events.destinationDirectory, withIntermediateDirectories: true)
            try! data.write(to: destination)
            try? FileManager.default.removeItem(at: first.store.partialURL("publish"))
            let second = Harness(directory: directory)
            check(second.downloader.start(parent: task, notificationConfig: nil), "interrupted publication handled")
            check(second.events.last("publish") == .complete && second.transport.started.isEmpty,
                  "published file completes without downloading again")
            check(second.store.loadAll().isEmpty, "completed state removed")

            let third = Harness(directory: root.appendingPathComponent("republish"))
            third.downloader.start(parent: parent("republish", size: Int64(mib)), notificationConfig: nil)
            let partial = third.store.partialURL("republish")
            try! data.write(to: partial)
            var pending = third.downloader.state("republish")!
            pending.publishing = true
            pending.committed = Int64(mib)
            pending.pieceTaskId = nil
            third.store.save(pending)
            let fourth = Harness(directory: root.appendingPathComponent("republish"))
            fourth.downloader.restoreAfterLaunch()
            check(fourth.events.last("republish") == .complete && output(fourth, "republish") == data,
                  "partial still present publishes again")
        }

        // A partial file without a state is removed at load.
        do {
            let directory = root.appendingPathComponent("orphan")
            try! FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            try! Data(repeating: 1, count: 10).write(to: directory.appendingPathComponent("lost.partial"))
            try! Data("{not json".utf8).write(to: directory.appendingPathComponent("broken.json"))
            try! Data(repeating: 2, count: 10).write(to: directory.appendingPathComponent("broken.partial"))
            let h = Harness(directory: directory)
            check(!h.downloader.hasDownloads, "unreadable state discarded")
            check(((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []).isEmpty,
                  "orphaned and unreadable partials removed")
        }

        // Group cancellation, untracked cleanup and WiFi policy changes.
        do {
            let h = Harness(directory: root.appendingPathComponent("admin"))
            h.downloader.start(parent: parent("admin-a", size: Int64(2 * mib)), notificationConfig: nil)
            h.downloader.start(parent: parent("admin-b", size: Int64(2 * mib)), notificationConfig: nil)
            h.downloader.start(parent: parent("admin-c", size: Int64(2 * mib)).copyWith(group: "movie"), notificationConfig: nil)
            let bPiece = h.downloader.state("admin-b")!.pieceTaskId!
            h.transport.wifiViolations = [bPiece]
            h.downloader.reapplyWiFiPolicy()
            check(h.transport.canceled == [bPiece] && h.downloader.state("admin-b")!.pieceTaskId != bPiece,
                  "only the piece under the old WiFi policy restarts")
            check(h.downloader.cancelAll(group: "serie") == 2, "group cancel counts its downloads")
            check(!h.downloader.owns(parentTaskId: "admin-a") && h.downloader.owns(parentTaskId: "admin-c"), "other groups kept")
            let statuses = h.events.statuses.count
            h.downloader.retainOnly(parentTaskIds: [])
            check(h.downloader.owns(parentTaskId: "admin-c"), "a download started by this process is never discarded")
            let relaunched = Harness(directory: root.appendingPathComponent("admin"))
            relaunched.downloader.retainOnly(parentTaskIds: [])
            check(!relaunched.downloader.owns(parentTaskId: "admin-c") && relaunched.store.loadAll().isEmpty,
                  "after a relaunch, untracked downloads are discarded")
            check(h.events.statuses.count == statuses && relaunched.events.statuses.isEmpty, "discarding reports nothing")
        }

        // Existing destination is replaced only by a complete, exact file.
        do {
            let h = Harness(directory: root.appendingPathComponent("replace"))
            let data = source(mib)
            try! FileManager.default.createDirectory(at: h.events.destinationDirectory, withIntermediateDirectories: true)
            try! Data("old".utf8).write(to: h.events.destinationDirectory.appendingPathComponent("replace.mkv"))
            h.downloader.start(parent: parent("replace", size: Int64(mib)), notificationConfig: nil)
            deliver(h, data, body: Data(repeating: 1, count: mib - 1))
            check(output(h, "replace") == Data("old".utf8), "short piece never published")
            deliver(h, data)
            check(output(h, "replace") == data, "complete file replaces the old one")
        }

        // A refused first piece is reported once, by the caller; a later refusal halts.
        do {
            let h = Harness(directory: root.appendingPathComponent("refuse"))
            h.transport.refuse = true
            check(!h.downloader.start(parent: parent("refuse", size: 10), notificationConfig: nil), "start refused")
            check(h.events.statuses.isEmpty && h.store.loadAll().isEmpty, "refused start leaves nothing and reports nothing")
            h.transport.refuse = false
            let data = source(2 * mib)
            h.downloader.start(parent: parent("refuse-later", size: Int64(2 * mib)), notificationConfig: nil)
            h.transport.refuse = true
            deliver(h, data)
            check(h.events.last("refuse-later") == .failed && h.store.partialSize("refuse-later") == Int64(mib),
                  "later refusal fails but keeps bytes")
        }

        print("Range fixtures passed: \(assertions) assertions")
    }
}
