//
//  RangeDownloaderPlugin.swift
//  background_downloader (IPTV local patch, see LOCAL_PATCHES.md)
//
//  Connects RangeDownloader to the plugin's background URLSession, Dart
//  updates, notifications, holding queue and application lifecycle.
//

import Foundation
import UIKit
import os.log

/// Runs piece transfers on the plugin's background URLSession.
final class URLSessionRangeTransport: RangeTransport {
    private let lock = NSLock()
    private var tasks = [String: URLSessionDownloadTask]()

    func start(piece: Task, earliestBegin: Date?) -> Bool {
        UrlSessionDelegate.createUrlSession()
        guard let session = UrlSessionDelegate.urlSession, let url = URL(string: piece.url) else { return false }
        var request = URLRequest(url: url)
        for (key, value) in piece.headers {
            request.setValue(value, forHTTPHeaderField: key)
        }
        if taskRequiresWiFi(task: piece) {
            request.allowsCellularAccess = false
        }
        // A stalled piece must fail quickly so a fresh attempt replaces it; the
        // session-wide request timeout can be much longer.
        request.timeoutInterval = 120
        let task = session.downloadTask(with: request)
        task.taskDescription = jsonStringFor(task: piece)
        task.priority = 1 - Float(piece.priority) / 10
        if let earliestBegin = earliestBegin {
            task.earliestBeginDate = earliestBegin
        }
        lock.withLock {
            // Forget finished transfers so their objects are not retained.
            tasks = tasks.filter { $0.value.state != .completed }
            tasks[piece.taskId] = task
        }
        task.resume()
        return true
    }

    func cancel(pieceTaskId: String) {
        if let task = lock.withLock({ tasks.removeValue(forKey: pieceTaskId) }) {
            task.cancel()
            return
        }
        UrlSessionDelegate.createUrlSession()
        UrlSessionDelegate.urlSession?.getAllTasks { sessionTasks in
            sessionTasks.filter { getTaskFrom(urlSessionTask: $0)?.taskId == pieceTaskId }.forEach { $0.cancel() }
        }
    }

    func liveTaskIds(_ completion: @escaping (Set<String>) -> Void) {
        UrlSessionDelegate.createUrlSession()
        guard let session = UrlSessionDelegate.urlSession else {
            completion([])
            return
        }
        session.getAllTasks { sessionTasks in
            var live = Set<String>()
            for case let task as URLSessionDownloadTask in sessionTasks where task.state == .running || task.state == .suspended {
                guard let piece = getTaskFrom(urlSessionTask: task), rangeChunkMetadata(piece) != nil else { continue }
                live.insert(piece.taskId)
                self.lock.withLock { self.tasks[piece.taskId] = task }
            }
            completion(live)
        }
    }

    func violatesWiFiPolicy(pieceTaskId: String) -> Bool {
        guard let task = lock.withLock({ tasks[pieceTaskId] }), task.state != .completed,
              let piece = getTaskFrom(urlSessionTask: task) else { return false }
        // Cellular access must be off exactly when WiFi is required.
        return (task.originalRequest?.allowsCellularAccess ?? true) == taskRequiresWiFi(task: piece)
    }
}

/// Reports parent changes through the plugin's normal channels.
final class PluginRangeEvents: RangeEvents {
    func status(_ parent: Task, _ status: TaskStatus, exception: TaskException?, responseStatusCode: Int?) {
        if status == .running {
            // Range parents can always resume: completes Dart's canResume
            // completer and offers Pause on the running notification.
            processCanResume(task: parent, taskCanResume: true)
            if parent.allowPause {
                BDPlugin.propertyLock.withLock { _ = BDPlugin.taskIdsThatCanResume.insert(parent.taskId) }
            }
        }
        processStatusUpdate(task: parent, status: status, taskException: exception, responseStatusCode: responseStatusCode)
    }

    func progress(_ parent: Task, done: Int64, total: Int64) {
        updateProgress(task: parent, totalBytesExpected: total, totalBytesDone: done)
    }

    func resumable(_ parent: Task) {
        // Dart keeps this marker as the parent's ResumeData; resuming re-enqueues
        // the parent, which continues from the persisted committed length.
        _ = processResumeData(task: parent, resumeData: Data("range".utf8))
    }

    func notice(_ parent: Task, _ notice: RangeNotice, notificationConfig: String?) {
        guard let json = notificationConfig, let config = notificationConfigFrom(jsonString: json) else { return }
        let type: NotificationType
        switch notice {
        case .running: type = .running
        case .complete: type = .complete
        case .error: type = .error
        case .paused: type = .paused
        case .canceled: type = .canceled
        }
        updateNotification(task: parent, notificationType: type, notificationConfig: config)
    }

    func finished(_ parent: Task) {
        guard let queue = BDPlugin.holdingQueue else { return }
        _Concurrency.Task {
            await queue.stateLock.lock()
            let held = queue.enqueuedTaskIds.contains(parent.taskId)
            await queue.stateLock.unlock()
            if held {
                await queue.taskFinished(parent)
            }
        }
    }

    func destination(for parent: Task) -> URL? {
        getFilePath(for: parent).map { URL(fileURLWithPath: $0) }
    }

    func published(_ file: URL) {
        guard UserDefaults.standard.bool(forKey: BDPlugin.keyConfigExcludeFromCloudBackup) else { return }
        var file = file
        do {
            try file.setCloudBackup(exclude: true)
        } catch {
            os_log("Could not exclude from iCloud backup: %{public}@ - %{public}@", log: log, type: .info, file.path, error.localizedDescription)
        }
    }
}

/// Tracks whether the app is active, since pieces created otherwise are discretionary.
private final class RangeAppState {
    static let shared = RangeAppState()
    private let lock = NSLock()
    private var active = false
    var isActive: Bool { lock.withLock { active } }

    func install() {
        let center = NotificationCenter.default
        center.addObserver(forName: UIApplication.didBecomeActiveNotification, object: nil, queue: .main) { _ in
            self.lock.withLock { self.active = true }
            // Off the main thread: the controller may be busy with file work.
            DispatchQueue.global().async { RangeDownloader.shared.promoteOnForeground() }
        }
        for name in [UIApplication.willResignActiveNotification, UIApplication.didEnterBackgroundNotification] {
            center.addObserver(forName: name, object: nil, queue: .main) { _ in
                self.lock.withLock { self.active = false }
            }
        }
        DispatchQueue.main.async {
            let active = UIApplication.shared.applicationState == .active
            self.lock.withLock { self.active = active }
        }
    }
}

private var rangeDownloaderInstalled = false

/// Called when the plugin registers; later registrations (other engines) keep
/// the first installation.
func installRangeDownloader() {
    guard !rangeDownloaderInstalled else { return }
    rangeDownloaderInstalled = true
    let downloader = RangeDownloader.shared
    downloader.transport = URLSessionRangeTransport()
    downloader.events = PluginRangeEvents()
    downloader.isAppActive = { RangeAppState.shared.isActive }
    RangeAppState.shared.install()
    if downloader.hasDownloads {
        // Reconnect the session so persisted downloads continue after a relaunch.
        UrlSessionDelegate.createUrlSession()
    }
}
