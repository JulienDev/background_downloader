//
//  RangeDownloader.swift
//  background_downloader (IPTV local patch, see LOCAL_PATCHES.md)
//
//  Sequential, resumable range download for a ParallelDownloadTask that the app
//  marked after proving byte-range support. Each piece is a short background
//  URLSession download of the parent's original URL, so every attempt obtains a
//  fresh provider redirect. Completed pieces are appended to one partial file
//  whose committed length is persisted; the download survives retries, pauses,
//  app suspension and process termination without Dart involvement.
//

import Foundation
import os.log

// MARK: - Markers and validation

/// Only the app's bounded GET Range probe may mark a parent as proven.
func provenRangeLength(_ task: Task) -> Int64? {
    guard let data = task.metaData.data(using: .utf8),
          let metadata = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          metadata["rangeChunks"] as? Bool == true,
          let value = task.headers.first(where: { $0.key.lowercased() == "known-content-length" })?.value,
          let length = Int64(value), length > 0 else { return nil }
    return length
}

let defaultRangePieceBytes: Int64 = 64 << 20
/// Pieces created while the app is inactive are this many times larger, so iOS
/// (which starts such transfers at its discretion) schedules fewer of them.
let backgroundPieceMultiplier: Int64 = 4
let minimumRangePieceBytes: Int64 = 1 << 20

/// Piece size requested by the app (`rangePieceBytes`), bounded to at least 1 MiB.
func rangePieceBytes(_ task: Task) -> Int64 {
    guard let data = task.metaData.data(using: .utf8),
          let metadata = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let value = (metadata["rangePieceBytes"] as? NSNumber)?.int64Value else { return defaultRangePieceBytes }
    return max(value, minimumRangePieceBytes)
}

/// Metadata of a range piece task, or nil for every other task.
func rangeChunkMetadata(_ task: Task) -> [String: String]? {
    guard task.group == chunkGroup, let data = task.metaData.data(using: .utf8),
          let metadata = try? JSONDecoder().decode([String: String].self, from: data),
          metadata["rangeTotal"] != nil, metadata["parentTaskId"] != nil else { return nil }
    return metadata
}

/// Returns why a piece response cannot be committed, or nil when it is exact.
func rangeChunkResponseError(_ task: Task, response: HTTPURLResponse, fileSize: Int64? = nil) -> String? {
    guard let metadata = rangeChunkMetadata(task) else { return nil }
    guard let first = Int64(metadata["from"] ?? ""), let last = Int64(metadata["to"] ?? ""),
          let total = Int64(metadata["rangeTotal"] ?? ""), first >= 0, last >= first, total > last else {
        return "Invalid planned chunk range: \(task.metaData)"
    }
    let expected = "bytes \(first)-\(last)/\(total)"
    // After a cut, nsurlsessiond may resume the piece itself: its last response
    // then starts later in the planned range, and the file it delivers holds the
    // earlier bytes followed by the resumed ones. The exact final size below
    // proves that concatenation.
    let contentRange = response.value(forHTTPHeaderField: "Content-Range")?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
    let start = contentRange.hasPrefix("bytes ") && contentRange.hasSuffix("-\(last)/\(total)")
        ? Int64(contentRange.dropFirst(6).prefix(while: { $0 != "-" })) : nil
    guard response.statusCode == 206, let start = start, start >= first, start <= last else {
        return "Expected 206 \(expected), received HTTP \(response.statusCode), headers \(response.allHeaderFields)"
    }
    if let encoding = response.value(forHTTPHeaderField: "Content-Encoding"), encoding.lowercased() != "identity" {
        return "Unexpected chunk Content-Encoding: \(encoding)"
    }
    let size = last - start + 1
    if let length = response.value(forHTTPHeaderField: "Content-Length"), Int64(length) != size {
        return "Expected chunk Content-Length \(size), received \(length)"
    }
    if let fileSize = fileSize, fileSize != last - first + 1 {
        return "Expected chunk size \(last - first + 1), received \(fileSize) (response \(contentRange))"
    }
    return nil
}

// MARK: - Persistent state

struct RangeDownloadState: Codable {
    var parent: Task
    var notificationConfig: String?
    var total: Int64
    var pieceBytes: Int64
    /// Bytes durably appended to the partial file.
    var committed: Int64 = 0
    /// Committed length before the latest piece, a known-good fallback.
    var previousCommitted: Int64 = 0
    /// Highest progress shown to the user; never decreases.
    var displayed: Int64 = 0
    /// Attempt counter, making every piece task id unique.
    var attempt: Int = 0
    /// Retries left for the current piece; refilled after each committed piece.
    var retriesRemaining: Int
    var paused = false
    /// Kept after a final failure so a retry continues from [committed].
    var failed = false
    var runningReported = false
    /// Task id of the piece in flight, if any.
    var pieceTaskId: String? = nil
    /// True when the piece was created while the app was not active (discretionary).
    var pieceStartedInBackground = false
    /// Set just before the complete partial file is moved to its destination.
    var publishing = false
    /// Identifies this download instance; a restart of the same task gets a new one.
    var generation = UUID().uuidString

    init(parent: Task, notificationConfig: String?, total: Int64) {
        self.parent = parent
        self.notificationConfig = notificationConfig
        self.total = total
        self.pieceBytes = rangePieceBytes(parent)
        self.retriesRemaining = parent.retries
    }

    /// Decodes states saved by earlier versions: fields added later default.
    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        parent = try c.decode(Task.self, forKey: .parent)
        notificationConfig = try c.decodeIfPresent(String.self, forKey: .notificationConfig)
        total = try c.decode(Int64.self, forKey: .total)
        pieceBytes = try c.decode(Int64.self, forKey: .pieceBytes)
        committed = try c.decodeIfPresent(Int64.self, forKey: .committed) ?? 0
        previousCommitted = try c.decodeIfPresent(Int64.self, forKey: .previousCommitted) ?? 0
        displayed = try c.decodeIfPresent(Int64.self, forKey: .displayed) ?? committed
        attempt = try c.decodeIfPresent(Int.self, forKey: .attempt) ?? 0
        retriesRemaining = try c.decodeIfPresent(Int.self, forKey: .retriesRemaining) ?? parent.retries
        paused = try c.decodeIfPresent(Bool.self, forKey: .paused) ?? false
        failed = try c.decodeIfPresent(Bool.self, forKey: .failed) ?? false
        runningReported = try c.decodeIfPresent(Bool.self, forKey: .runningReported) ?? false
        pieceTaskId = try c.decodeIfPresent(String.self, forKey: .pieceTaskId)
        pieceStartedInBackground = try c.decodeIfPresent(Bool.self, forKey: .pieceStartedInBackground) ?? false
        publishing = try c.decodeIfPresent(Bool.self, forKey: .publishing) ?? false
        generation = try c.decodeIfPresent(String.self, forKey: .generation) ?? UUID().uuidString
    }

    /// Paused or failed: nothing runs until the parent is enqueued again.
    var halted: Bool { paused || failed }

    /// Next byte range of at most [bytes] (default one piece), or nil once every
    /// byte is committed.
    func nextPiece(bytes: Int64? = nil) -> (from: Int64, to: Int64)? {
        guard committed < total else { return nil }
        return (committed, min(committed + (bytes ?? pieceBytes), total) - 1)
    }
}

/// Builds the background task for one piece: the parent's URL and headers, an
/// exact Range and identity encoding. Its metadata identifies parent and range.
func rangePieceTask(_ state: RangeDownloadState, from: Int64, to: Int64) -> Task {
    let parent = state.parent
    var headers = parent.headers.filter {
        !["known-content-length", "accept-encoding", "range"].contains($0.key.lowercased())
    }
    headers["Accept-Encoding"] = "identity"
    headers["Range"] = "bytes=\(from)-\(to)"
    // `group` lets the holding queue count the piece under its parent's group.
    let metadata = ["parentTaskId": parent.taskId, "from": String(from), "to": String(to),
                    "rangeTotal": String(state.total), "group": parent.group]
    let metaData = (try? JSONEncoder().encode(metadata)).map { String(decoding: $0, as: UTF8.self) } ?? ""
    return Task(taskId: "\(parent.taskId)-r\(state.attempt)", url: parent.url, filename: "\(parent.taskId)-r\(state.attempt).piece",
                headers: headers, baseDirectory: BaseDirectory.temporary.rawValue, group: chunkGroup,
                updates: Updates.statusChangeAndProgressUpdates.rawValue, requiresWiFi: parent.requiresWiFi,
                retries: 0, retriesRemaining: 0, allowPause: false, priority: parent.priority,
                metaData: metaData, taskType: "DownloadTask")
}

/// Stores each download's state and partial file under one directory.
final class RangeStore {
    let directory: URL

    init(directory: URL) {
        self.directory = directory
    }

    static func defaultDirectory() -> URL {
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? URL(fileURLWithPath: NSTemporaryDirectory())
        return support.appendingPathComponent("com.bbflight.background_downloader/range", isDirectory: true)
    }

    func stateURL(_ id: String) -> URL { directory.appendingPathComponent("\(id).json") }
    func partialURL(_ id: String) -> URL { directory.appendingPathComponent("\(id).partial") }

    func loadAll() -> [String: RangeDownloadState] {
        let files = (try? FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)) ?? []
        var states = [String: RangeDownloadState]()
        for file in files where file.pathExtension == "json" {
            guard let data = try? Data(contentsOf: file),
                  let state = try? JSONDecoder().decode(RangeDownloadState.self, from: data) else {
                os_log("Discarding unreadable range state %{public}@", log: log, type: .error, file.path)
                remove(file.deletingPathExtension().lastPathComponent)
                continue
            }
            states[state.parent.taskId] = state
        }
        // A partial file without a state can never be resumed or published.
        for file in files where file.pathExtension == "partial" {
            let id = file.deletingPathExtension().lastPathComponent
            if states[id] == nil {
                os_log("Removing orphaned range partial %{public}@", log: log, type: .info, file.path)
                try? FileManager.default.removeItem(at: file)
            }
        }
        return states
    }

    func save(_ state: RangeDownloadState) {
        do {
            try ensureDirectory()
            try JSONEncoder().encode(state).write(to: stateURL(state.parent.taskId), options: .atomic)
        } catch {
            os_log("Could not save range state %{public}@: %{public}@", log: log, type: .error, state.parent.taskId, error.localizedDescription)
        }
    }

    func remove(_ id: String) {
        try? FileManager.default.removeItem(at: stateURL(id))
        try? FileManager.default.removeItem(at: partialURL(id))
    }

    func partialSize(_ id: String) -> Int64 {
        ((try? FileManager.default.attributesOfItem(atPath: partialURL(id).path))?[.size] as? NSNumber)?.int64Value ?? 0
    }

    /// Aligns the partial file with the committed length after an interruption:
    /// bytes beyond it (an interrupted append) are dropped; a shorter file falls
    /// back to the commit before the latest piece.
    func reconcile(_ state: inout RangeDownloadState) {
        let id = state.parent.taskId
        let size = partialSize(id)
        if size != state.committed {
            // Longer: an interrupted append, cut back to the commit. Shorter: the
            // latest commit was not durable, so fall back to the one before.
            let aligned = size > state.committed ? state.committed : min(size, state.previousCommitted)
            os_log("Reconciling range partial %{public}@: size=%lld committed=%lld kept=%lld", log: log, type: .info,
                   id, size, state.committed, aligned)
            truncatePartial(id, to: aligned)
            state.committed = aligned
        }
        state.displayed = max(state.displayed, state.committed)
    }

    /// Appends a verified piece file and returns the new committed length.
    func append(_ location: URL, to state: RangeDownloadState, expected: Int64) throws -> Int64 {
        try ensureDirectory()
        let partial = partialURL(state.parent.taskId)
        if state.committed == 0 {
            // The first piece becomes the partial file: a move, not a copy.
            try? FileManager.default.removeItem(at: partial)
            try FileManager.default.moveItem(at: location, to: partial)
            excludeFromBackup(partial)
            guard partialSize(state.parent.taskId) == expected else {
                try? FileManager.default.removeItem(at: partial)
                throw CocoaError(.fileReadCorruptFile)
            }
            return expected
        }
        // Committed bytes must still be on disk; never extend a missing file.
        guard partialSize(state.parent.taskId) >= state.committed else { throw CocoaError(.fileReadCorruptFile) }
        let output = try FileHandle(forWritingTo: partial)
        defer { try? output.close() }
        try output.truncate(atOffset: UInt64(state.committed))
        try output.seek(toOffset: UInt64(state.committed))
        let input = try FileHandle(forReadingFrom: location)
        defer { try? input.close() }
        var copied: Int64 = 0
        while let data = try input.read(upToCount: 1 << 20), !data.isEmpty {
            try output.write(contentsOf: data)
            copied += Int64(data.count)
        }
        try output.synchronize()
        guard copied == expected, try output.offset() == UInt64(state.committed + expected) else {
            try? output.truncate(atOffset: UInt64(state.committed))
            throw CocoaError(.fileReadCorruptFile)
        }
        return state.committed + expected
    }

    /// Moves the complete partial file to [destination], replacing any file there.
    func publish(_ state: RangeDownloadState, to destination: URL) throws {
        let partial = partialURL(state.parent.taskId)
        guard partialSize(state.parent.taskId) == state.total else { throw CocoaError(.fileReadCorruptFile) }
        try FileManager.default.createDirectory(at: destination.deletingLastPathComponent(), withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: destination.path) {
            try FileManager.default.removeItem(at: destination)
        }
        try FileManager.default.moveItem(at: partial, to: destination)
    }

    private func truncatePartial(_ id: String, to length: Int64) {
        guard let handle = try? FileHandle(forWritingTo: partialURL(id)) else { return }
        try? handle.truncate(atOffset: UInt64(length))
        try? handle.close()
    }

    private func ensureDirectory() throws {
        if !FileManager.default.fileExists(atPath: directory.path) {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            excludeFromBackup(directory)
        }
    }

    private func excludeFromBackup(_ url: URL) {
        var url = url
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        try? url.setResourceValues(values)
    }
}

// MARK: - Platform boundaries

/// Starts and cancels piece transfers (background URLSession in production).
protocol RangeTransport: AnyObject {
    /// Starts [piece], not before [earliestBegin] when given. Returns false if it cannot.
    func start(piece: Task, earliestBegin: Date?) -> Bool
    func cancel(pieceTaskId: String)
    /// Task ids of the piece transfers the session still owns.
    func liveTaskIds(_ completion: @escaping (Set<String>) -> Void)
    /// True when a live piece was started under a different WiFi policy.
    func violatesWiFiPolicy(pieceTaskId: String) -> Bool
}

enum RangeNotice {
    case running, complete, error, paused, canceled
}

/// Reports parent changes to the plugin (Dart updates, notifications, queue).
protocol RangeEvents: AnyObject {
    func status(_ parent: Task, _ status: TaskStatus, exception: TaskException?, responseStatusCode: Int?)
    func progress(_ parent: Task, done: Int64, total: Int64)
    /// Records that [parent] can resume (Dart keeps this as its ResumeData).
    func resumable(_ parent: Task)
    func notice(_ parent: Task, _ notice: RangeNotice, notificationConfig: String?)
    func finished(_ parent: Task)
    func destination(for parent: Task) -> URL?
    func published(_ file: URL)
}


// MARK: - Controller

final class RangeDownloader {
    static let shared = RangeDownloader(store: RangeStore(directory: RangeStore.defaultDirectory()))

    let store: RangeStore
    var transport: RangeTransport?
    var events: RangeEvents?
    var isAppActive: () -> Bool = { true }
    var now: () -> Date = Date.init
    /// Delay before restarting pieces after launch, so pending completions arrive first.
    var restoreDelay: (@escaping () -> Void) -> Void = { work in
        DispatchQueue.global().asyncAfter(deadline: .now() + 2, execute: work)
    }
    /// Delay after activation before judging whether a background piece receives data.
    var promoteDelay: (@escaping () -> Void) -> Void = { work in
        DispatchQueue.global().asyncAfter(deadline: .now() + 3, execute: work)
    }

    private let lock = NSRecursiveLock()
    private var loaded = false
    private var states = [String: RangeDownloadState]()
    private var abandoned = Set<String>()
    private var validated = Set<String>()
    private var lastSave = [String: Date]()
    /// Time of the latest progress callback of each piece.
    private var lastData = [String: Date]()
    /// Downloads started by this process: Dart may not have recorded them yet.
    private var startedThisProcess = Set<String>()
    private var depth = 0
    private var pending = [() -> Void]()

    init(store: RangeStore) {
        self.store = store
    }

    /// Runs [body] under the lock. Events queued meanwhile run after unlocking:
    /// posting to Dart may wait for the main thread, which may wait for this lock.
    private func locked<T>(_ body: () -> T) -> T {
        lock.lock()
        depth += 1
        if !loaded {
            states = store.loadAll()
            for id in Array(states.keys) where !states[id]!.publishing {
                store.reconcile(&states[id]!)
                store.save(states[id]!)
            }
            loaded = true
        }
        let result = body()
        depth -= 1
        var work = [() -> Void]()
        if depth == 0 {
            work = pending
            pending.removeAll()
        }
        lock.unlock()
        work.forEach { $0() }
        return result
    }

    /// Queues a notification to the plugin; delivered once the lock is released.
    private func emit(_ work: @escaping (RangeEvents) -> Void) {
        guard let events = events else { return }
        pending.append { work(events) }
    }

    // MARK: Queries

    var hasDownloads: Bool { locked { !states.isEmpty } }

    func owns(parentTaskId: String) -> Bool { locked { states[parentTaskId] != nil } }

    func parent(taskId: String) -> Task? { locked { states[taskId]?.parent } }

    /// Parents that are neither paused, failed nor finished, for task reconciliation.
    func activeParents() -> [Task] { locked { states.values.filter { !$0.halted }.map { $0.parent } } }

    func state(_ parentTaskId: String) -> RangeDownloadState? { locked { states[parentTaskId] } }

    // MARK: Commands

    /// Starts [parent], or continues its persisted download (resume, retry after
    /// a failure, rescheduled enqueue). Returns false if nothing could start.
    @discardableResult
    func start(parent: Task, notificationConfig: String?) -> Bool {
        guard let total = provenRangeLength(parent) else { return false }
        return locked {
            let id = parent.taskId
            if states[id]?.publishing == true, finishInterruptedPublish(id) {
                return true
            }
            if var state = states[id], state.total == total {
                if state.halted {
                    state.retriesRemaining = parent.retries
                }
                state.paused = false
                state.failed = false
                // Dart marked the re-enqueued parent `enqueued`; report `running` again.
                state.runningReported = false
                state.parent = parent
                state.notificationConfig = notificationConfig ?? state.notificationConfig
                states[id] = state
                save(id, force: true)
                os_log("Continuing range download %{public}@ at %lld of %lld", log: log, type: .info, id, state.committed, total)
                // Show the retained progress immediately, not 0 until new data.
                emit { $0.progress(parent, done: state.displayed, total: total) }
                if let piece = state.pieceTaskId {
                    // A live transfer keeps going; otherwise start the next piece.
                    transport?.liveTaskIds { live in
                        self.locked {
                            if !live.contains(piece), self.states[id]?.pieceTaskId == piece, self.states[id]?.halted == false {
                                self.states[id]?.pieceTaskId = nil
                                self.advance(id, earliestBegin: nil)
                            }
                        }
                    }
                } else {
                    advance(id, earliestBegin: nil)
                }
                return true
            }
            store.remove(id)
            states[id] = RangeDownloadState(parent: parent, notificationConfig: notificationConfig, total: total)
            startedThisProcess.insert(id)
            os_log("Starting range download %{public}@ of %lld bytes, pieces of %lld", log: log, type: .info,
                   id, total, states[id]!.pieceBytes)
            guard startPiece(id, earliestBegin: nil) else {
                // Nothing was reported yet: the caller reports the refused enqueue.
                states.removeValue(forKey: id)
                store.remove(id)
                lastSave.removeValue(forKey: id)
                return false
            }
            return true
        }
    }

    /// Pauses [parentTaskId]: the piece in flight is dropped, committed bytes kept.
    func pause(parentTaskId id: String) -> Bool {
        locked {
            guard var state = states[id], !state.halted else { return false }
            if let piece = state.pieceTaskId { abandon(piece) }
            state.pieceTaskId = nil
            state.paused = true
            states[id] = state
            save(id, force: true)
            os_log("Paused range download %{public}@ at %lld", log: log, type: .info, id, state.committed)
            emit { $0.resumable(state.parent) }
            emit { $0.status(state.parent, .paused, exception: nil, responseStatusCode: nil) }
            emit { $0.notice(state.parent, .paused, notificationConfig: state.notificationConfig) }
            emit { $0.finished(state.parent) }
            return true
        }
    }

    /// Cancels [parentTaskId] in any state and deletes its partial file.
    func cancel(parentTaskId id: String) -> Bool {
        locked {
            guard let state = states[id] else { return false }
            if let piece = state.pieceTaskId { abandon(piece) }
            remove(id, .canceled)
            return true
        }
    }

    /// Cancels every download of [group]; returns how many there were.
    func cancelAll(group: String) -> Int {
        locked {
            let ids = states.filter { $0.value.parent.group == group }.map { $0.key }
            ids.forEach { _ = cancel(parentTaskId: $0) }
            return ids.count
        }
    }

    /// Silently discards downloads whose parents Dart no longer tracks.
    func retainOnly(parentTaskIds: Set<String>) {
        locked {
            for (id, state) in states where !parentTaskIds.contains(id) && !startedThisProcess.contains(id) {
                os_log("Discarding untracked range download %{public}@ (%lld bytes)", log: log, type: .info, id, state.committed)
                if let piece = state.pieceTaskId { abandon(piece) }
                states.removeValue(forKey: id)
                store.remove(id)
                lastSave.removeValue(forKey: id)
            }
        }
    }

    /// Restarts live pieces whose WiFi requirement no longer matches the setting.
    func reapplyWiFiPolicy() {
        locked {
            for (id, state) in states where !state.halted {
                guard let piece = state.pieceTaskId, transport?.violatesWiFiPolicy(pieceTaskId: piece) == true else { continue }
                os_log("Restarting range piece %{public}@ for the new WiFi requirement", log: log, type: .info, piece)
                abandon(piece)
                states[id]?.pieceTaskId = nil
                advance(id, earliestBegin: nil)
            }
        }
    }

    // MARK: Session callbacks (return true when the task is a range piece)

    func handleProgress(piece: Task, response: HTTPURLResponse?, totalBytesWritten: Int64) -> Bool {
        guard let metadata = rangeChunkMetadata(piece) else { return false }
        locked {
            guard let id = metadata["parentTaskId"], var state = states[id], state.pieceTaskId == piece.taskId,
                  !abandoned.contains(piece.taskId) else {
                if !abandoned.contains(piece.taskId) { abandon(piece.taskId) } // orphan or stale attempt
                return
            }
            if !validated.contains(piece.taskId), let response = response {
                if let error = rangeChunkResponseError(piece, response: response) {
                    abandon(piece.taskId)
                    pieceFailed(id, reason: error, httpCode: response.statusCode)
                    return
                }
                validated.insert(piece.taskId)
            }
            lastData[piece.taskId] = now()
            if !state.runningReported {
                state.runningReported = true
                emit { $0.status(state.parent, .running, exception: nil, responseStatusCode: nil) }
                emit { $0.notice(state.parent, .running, notificationConfig: state.notificationConfig) }
            }
            state.displayed = max(state.displayed, min(state.committed + totalBytesWritten, state.total))
            states[id] = state
            emit { $0.progress(state.parent, done: state.displayed, total: state.total) }
            save(id, force: false)
        }
        return true
    }

    /// Must run before the session deletes [location]. The file is appended
    /// outside the lock, so the main thread never waits for a large copy.
    func handleFinished(piece: Task, response: HTTPURLResponse?, location: URL) -> Bool {
        guard let metadata = rangeChunkMetadata(piece) else { return false }
        let plan: (id: String, state: RangeDownloadState, size: Int64)? = locked {
            guard let id = metadata["parentTaskId"], let state = states[id], state.pieceTaskId == piece.taskId,
                  !abandoned.contains(piece.taskId) else { return nil }
            validated.remove(piece.taskId)
            lastData.removeValue(forKey: piece.taskId)
            let size = ((try? FileManager.default.attributesOfItem(atPath: location.path))?[.size] as? NSNumber)?.int64Value ?? -1
            guard let response = response else {
                pieceFailed(id, reason: "No HTTP response for \(piece.url)", httpCode: nil)
                return nil
            }
            if let error = rangeChunkResponseError(piece, response: response, fileSize: size) {
                let body = response.statusCode >= 400 ? (try? String(contentsOf: location, encoding: .utf8)) ?? "" : ""
                pieceFailed(id, reason: "\(error) url=\(piece.url) body=\(body)", httpCode: response.statusCode)
                return nil
            }
            return (id, state, size)
        }
        guard let plan = plan else { return true }
        let appended = Result { try store.append(location, to: plan.state, expected: plan.size) }
        locked {
            guard var state = states[plan.id], state.generation == plan.state.generation else {
                // Canceled (or restarted as a new download) while appending:
                // these bytes belong to no current download.
                if states[plan.id] == nil { store.remove(plan.id) }
                return
            }
            guard state.committed == plan.state.committed else {
                // Another attempt already committed these bytes.
                if state.pieceTaskId == piece.taskId {
                    states[plan.id]?.pieceTaskId = nil
                    advance(plan.id, earliestBegin: nil)
                }
                return
            }
            switch appended {
            case .success(let committed):
                // A promotion or restore may have started another attempt of the
                // same range meanwhile; these verified bytes make it redundant.
                if let other = state.pieceTaskId, other != piece.taskId { abandon(other) }
                state.previousCommitted = state.committed
                state.committed = committed
                state.displayed = max(state.displayed, committed)
                state.pieceTaskId = nil
                state.retriesRemaining = state.parent.retries
                states[plan.id] = state
                save(plan.id, force: true)
                emit { $0.progress(state.parent, done: state.displayed, total: state.total) }
                // A pause during the append keeps the bytes but starts nothing.
                if !state.halted {
                    advance(plan.id, earliestBegin: nil)
                }
            case .failure(let error):
                guard state.pieceTaskId == piece.taskId else { return }
                states[plan.id]?.pieceTaskId = nil
                halt(plan.id, .failed, exception: TaskException(type: .fileSystem,
                     description: "Could not append range piece: \(error.localizedDescription)"))
            }
        }
        return true
    }

    func handleCompleted(piece: Task, error: Error?) -> Bool {
        guard let metadata = rangeChunkMetadata(piece) else { return false }
        locked {
            validated.remove(piece.taskId)
            lastData.removeValue(forKey: piece.taskId)
            if abandoned.remove(piece.taskId) != nil { return }
            guard let error = error, let id = metadata["parentTaskId"], states[id]?.pieceTaskId == piece.taskId else { return }
            pieceFailed(id, reason: "\(error.localizedDescription) url=\(piece.url) error=\(error)", httpCode: nil)
        }
        return true
    }

    // MARK: Lifecycle

    /// After a process launch: restarts pieces that the session no longer owns.
    func restoreAfterLaunch() {
        let snapshot: [String: String?] = locked {
            Dictionary(uniqueKeysWithValues: states.filter { !$0.value.halted }.map { ($0.key, $0.value.pieceTaskId) })
        }
        guard !snapshot.isEmpty, let transport = transport else { return }
        transport.liveTaskIds { live in
            self.restoreDelay {
                self.locked {
                    for (id, piece) in snapshot {
                        guard let state = self.states[id], !state.halted, state.pieceTaskId == piece else { continue }
                        if state.publishing, self.finishInterruptedPublish(id) { continue }
                        if let piece = piece, live.contains(piece) { continue }
                        os_log("Restoring range download %{public}@ at %lld", log: log, type: .info, id, state.committed)
                        // A late completion of the lost attempt must not race the new one.
                        if let piece = piece { self.abandon(piece) }
                        self.states[id]?.pieceTaskId = nil
                        self.advance(id, earliestBegin: nil)
                    }
                }
                if self.isAppActive() {
                    self.promoteOnForeground()
                }
            }
        }
    }

    /// Called when the app becomes active. iOS runs transfers created while the
    /// app was inactive at its discretion; a piece created then that delivers no
    /// data shortly after activation is recreated in the foreground. A transfer
    /// that is receiving reports progress within that delay and is left alone.
    func promoteOnForeground() {
        let activated = now()
        let candidates: [(String, String)] = locked {
            states.compactMap { id, state in
                !state.halted && state.pieceStartedInBackground ? state.pieceTaskId.map { (id, $0) } : nil
            }
        }
        guard !candidates.isEmpty else { return }
        promoteDelay {
            self.locked {
                for (id, piece) in candidates {
                    guard self.isAppActive(), let state = self.states[id], !state.halted, state.pieceTaskId == piece else { continue }
                    if let last = self.lastData[piece], last >= activated { continue }
                    os_log("Restarting discretionary range piece %{public}@ in the foreground", log: log, type: .info, piece)
                    self.abandon(piece)
                    self.states[id]?.pieceTaskId = nil
                    self.advance(id, earliestBegin: nil)
                }
            }
        }
    }

    // MARK: Internals

    /// Starts the next piece, or publishes the file once complete. Pieces are
    /// larger while the app is inactive and halve after each failure. Returns
    /// false only when the transport refuses the piece.
    @discardableResult
    private func startPiece(_ id: String, earliestBegin: Date?) -> Bool {
        guard var state = states[id], !state.halted else { return true }
        let active = isAppActive()
        // Larger pieces while inactive, bounded so a lost piece costs little.
        // Each consecutive failure halves the piece (down to 1 MiB), so a
        // provider that cuts often still lets small pieces through; a committed
        // piece restores the normal size.
        let failures = state.parent.retries - state.retriesRemaining
        let large = !active && failures == 0
        let bytes = large ? state.pieceBytes * backgroundPieceMultiplier
            : max(state.pieceBytes >> Int64(min(max(failures, 0), 16)), minimumRangePieceBytes)
        guard let range = state.nextPiece(bytes: bytes) else {
            complete(id)
            return true
        }
        state.attempt += 1
        let piece = rangePieceTask(state, from: range.from, to: range.to)
        state.pieceTaskId = piece.taskId
        state.pieceStartedInBackground = !active
        states[id] = state
        save(id, force: true)
        os_log("Range piece %{public}@ bytes=%lld-%lld of %lld url=%{public}@ delay=%f background=%d", log: log, type: .info,
               piece.taskId, range.from, range.to, state.total, piece.url,
               earliestBegin.map { $0.timeIntervalSince(now()) } ?? 0, large ? 1 : 0)
        guard transport?.start(piece: piece, earliestBegin: earliestBegin) == true else {
            states[id]?.pieceTaskId = nil
            return false
        }
        return true
    }

    /// Starts the next piece of a running download; a refused piece halts it.
    private func advance(_ id: String, earliestBegin: Date?) {
        if !startPiece(id, earliestBegin: earliestBegin) {
            halt(id, .failed, exception: TaskException(type: .url,
                 description: "Could not start range piece for \(states[id]?.parent.url ?? id)"))
        }
    }

    private func pieceFailed(_ id: String, reason: String, httpCode: Int?) {
        guard var state = states[id] else { return }
        state.pieceTaskId = nil
        os_log("Range piece failed for %{public}@ at %lld: %{public}@", log: log, type: .info, id, state.committed, reason)
        if httpCode == 404 {
            states[id] = state
            halt(id, .notFound, responseStatusCode: 404)
            return
        }
        guard state.retriesRemaining > 0 else {
            states[id] = state
            halt(id, .failed, exception: TaskException(type: httpCode == nil ? .connection : .httpResponse,
                                                       httpResponseCode: httpCode ?? -1, description: reason))
            return
        }
        state.retriesRemaining -= 1
        let delay = Double(2 << min(max(state.parent.retries - state.retriesRemaining - 1, 0), 8))
        states[id] = state
        advance(id, earliestBegin: now().addingTimeInterval(delay))
    }

    private func complete(_ id: String) {
        guard let state = states[id] else { return }
        guard let destination = events?.destination(for: state.parent) else {
            halt(id, .failed, exception: TaskException(type: .fileSystem, description: "No destination for \(state.parent.filename)"))
            return
        }
        do {
            states[id]?.publishing = true
            save(id, force: true)
            try store.publish(state, to: destination)
            emit { $0.published(destination) }
            os_log("Completed range download %{public}@ to %{public}@", log: log, type: .info, id, destination.path)
            remove(id, .complete, responseStatusCode: 206)
        } catch {
            states[id]?.publishing = false
            halt(id, .failed, exception: TaskException(type: .fileSystem,
                 description: "Could not publish \(destination.path): \(error.localizedDescription)"))
        }
    }

    /// After termination during publication: completes the download if the
    /// file reached its destination, else lets it publish again. Returns true
    /// when the download is now complete.
    private func finishInterruptedPublish(_ id: String) -> Bool {
        guard let state = states[id], state.publishing else { return false }
        let destination = events?.destination(for: state.parent)
        let size = destination.flatMap {
            ((try? FileManager.default.attributesOfItem(atPath: $0.path))?[.size] as? NSNumber)?.int64Value
        }
        if store.partialSize(id) == 0, size == state.total {
            os_log("Range download %{public}@ was published before termination", log: log, type: .info, id)
            remove(id, .complete, responseStatusCode: 206)
            return true
        }
        states[id]?.publishing = false
        store.reconcile(&states[id]!)
        save(id, force: true)
        return false
    }

    /// Ends the download for now while keeping its committed bytes: a retry by
    /// the user (re-enqueue or resume) continues from them; cancel deletes them.
    private func halt(_ id: String, _ status: TaskStatus, exception: TaskException? = nil, responseStatusCode: Int? = nil) {
        guard var state = states[id] else { return }
        if let piece = state.pieceTaskId { abandon(piece) }
        state.pieceTaskId = nil
        state.failed = true
        state.retriesRemaining = state.parent.retries
        states[id] = state
        save(id, force: true)
        os_log("Halted range download %{public}@ at %lld with %d: %{public}@", log: log, type: .info,
               id, state.committed, status.rawValue, exception?.description ?? "")
        emit { $0.resumable(state.parent) }
        emit { $0.status(state.parent, status, exception: exception, responseStatusCode: responseStatusCode) }
        emit { $0.notice(state.parent, .error, notificationConfig: state.notificationConfig) }
        emit { $0.finished(state.parent) }
    }

    /// Ends the download for good and deletes its state and partial file.
    private func remove(_ id: String, _ status: TaskStatus, exception: TaskException? = nil, responseStatusCode: Int? = nil) {
        guard let state = states.removeValue(forKey: id) else { return }
        store.remove(id)
        lastSave.removeValue(forKey: id)
        emit { $0.status(state.parent, status, exception: exception, responseStatusCode: responseStatusCode) }
        emit { $0.notice(state.parent, status == .complete ? .complete : .canceled, notificationConfig: state.notificationConfig) }
        emit { $0.finished(state.parent) }
    }

    private func abandon(_ piece: String) {
        abandoned.insert(piece)
        validated.remove(piece)
        lastData.removeValue(forKey: piece)
        transport?.cancel(pieceTaskId: piece)
    }

    private func save(_ id: String, force: Bool) {
        guard let state = states[id] else { return }
        let date = now()
        if !force, let last = lastSave[id], date.timeIntervalSince(last) < 5 { return }
        lastSave[id] = date
        store.save(state)
    }
}
