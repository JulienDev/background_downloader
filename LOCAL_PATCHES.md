# Range-download compatibility patches

This branch (`iptv/range-recovery-9.5.6`) is `background_downloader` 9.5.6
(upstream commit `f522f7d`) plus the IPTV range-download recovery patch. The
IPTV Flutter apps depend on it by git commit. Upstream files, including
`example/`, are otherwise unchanged; the git history is the provenance.

Patched files:

- iOS, under `ios/background_downloader/Sources/background_downloader/`: new
  `RangeDownloader.swift` and `RangeDownloaderPlugin.swift`, with hooks in
  `BDPlugin.swift`, `UrlSessionDelegate.swift`, `HoldingQueue.swift` and
  `WiFi.swift`. `ParallelDownloader.swift` is upstream.
- macOS/Windows: `lib/src/desktop/download_isolate.dart` and
  `lib/src/desktop/desktop_downloader.dart`.
- Android: `android/src/main/kotlin/com/bbflight/background_downloader/DownloadTaskRunner.kt`.
- Shared Dart: `lib/src/base_downloader.dart` (progress preservation, retry
  budget, retained failures, cancellation), `lib/src/chunk.dart`,
  `lib/src/native_downloader.dart` and `lib/src/file_downloader.dart`
  (`retainRangeDownloads`).

The native/Dart message format and unmarked task behavior remain upstream.

## Why

The tested Xtream provider redirects each request to a CDN URL with a token
that expires within seconds when unused and within 30 s after a transfer stops;
a reused token returns HTTP 406. Its `Accept-Ranges` header is malformed
(`0-<size>`), HEAD returns no length, and there is no ETag or Last-Modified.
Recovery therefore always restarts from the provider URL, so each attempt gets a
fresh redirect, and requests an exact byte range validated by the response.

## iOS

The app proves range support with a bounded GET `Range: bytes=0-0`,
`Accept-Encoding: identity`: a valid 206, an exact one-byte body and a positive
Content-Range total. It then enqueues a `ParallelDownloadTask` marked with
metadata `rangeChunks: true`, `rangePieceBytes` (64 MiB) and header
`Known-Content-Length: N`. Only this marked parent bypasses native
HEAD/Accept-Ranges checks.

`RangeDownloader` runs the marked parent natively, with no Dart round trip, one
background URLSession download at a time of the parent's URL with an exact
`Range` and identity encoding. `nsurlsessiond` reconnects to the redirected URL
by itself after a cut and then gets 406; a new piece always starts again from the
provider URL. A piece is committed only after a 206 with its Content-Range end
and total, identity encoding, a matching Content-Length and the exact planned
file size. A 200 ignoring Range is rejected on its first data. When the session
resumed a piece itself, its last response starts later in the planned range; it
is accepted only if the delivered file has the exact planned size. Committed
pieces are appended to one partial file under Application Support (excluded from
backup); the first piece is moved rather than copied, so disk use peaks at the
file size plus one piece. Appending runs outside the controller's lock, so the
main thread never waits for a copy; a cancellation meanwhile removes the file,
and a pause meanwhile still keeps the verified bytes. The final file is
published by a move only when its size equals the probed total; an existing
destination is replaced and the configured iCloud exclusion applies.

Pieces created while the app is active are 64 MiB. A piece created while the app
is inactive is four times larger (256 MiB), so iOS, which starts such transfers
at its discretion, schedules fewer of them while a lost piece stays bounded.
Each consecutive failure halves the piece, down to 1 MiB, so a provider that
cuts often still lets small pieces through; a committed piece restores the
normal size. Each piece request has a 120 s timeout, so a stalled transfer is
replaced quickly whatever the session-wide timeout. Displayed progress never decreases, so
after a lost piece it pauses until the new attempt passes it again. A failed piece is retried with a bounded range after 2, 4,
8 ... s through `earliestBeginDate`, with no app timer. The parent's `retries`
budget applies per piece and is refilled after each committed piece. Retries keep
the parent `running`, and reporting `running` also tells Dart the parent can
resume.

When a piece exhausts its budget, returns 404, or publication fails, the
download halts as `failed`/`notFound` but keeps its state and partial file and
posts a resume marker first. Dart keeps that marker as ResumeData, so a user
retry (resume, or tapping download again) continues from the committed bytes.
Pause drops only the piece in flight and posts the same marker. Cancel, in any
state (including a resumed parent still in the holding queue), deletes the state
and partial file. At app start the app calls
`retainRangeDownloads` with every tracked record, and native downloads without a
record are discarded silently, except downloads started by the current process,
which Dart may not have recorded yet.

Progress never decreases: the controller keeps the highest displayed byte count,
sends it as soon as a download continues, and Dart keeps the record's progress
for range parents while they are enqueued or running. Notifications follow the
running, paused, complete, error and canceled states; their Pause and Cancel
actions are routed to the controller, and Pause is offered while running.

The state is persisted at each piece and every 5 s of progress; states saved by
earlier versions decode with defaults for fields added since. Each state carries a generation, so bytes appended for a download that was
canceled and restarted meanwhile are never committed to the new one. If the
partial file is shorter than the committed length, the commit before the latest
piece is used. A `publishing` flag
is saved before the final move: after a termination, a file already at its
destination completes the download, otherwise publication runs again. Unreadable
states and partial files without a state are removed at load, and a restored
download abandons its lost attempt before starting a new one. After any
relaunch, including a background relaunch by the session or a rescheduled
enqueue from Dart, it is reloaded; an interrupted append is truncated to the
committed length and a piece the session no longer owns is restarted. Native
`allTasks` reports active parents instead of their pieces, so Dart's killed-task
rescheduling does not duplicate them. When the app becomes active, a piece
created while inactive that reports no data within 3 s is recreated in the
foreground; a piece receiving data is left alone.

Range pieces bypass the holding queue. The parent holds its slot until it
pauses, halts or finishes; the queue's periodic recount places live pieces under
their parent's group, and only tasks the queue started release a slot, with
counts never below zero. A WiFi requirement change restarts pieces started under
the old policy, and `reset(group)` cancels that group's range downloads. Events
to Dart and notifications are queued under the controller's lock and delivered
after it is released, because posting to Dart can wait for the main thread,
which may itself need that lock.

`test/native/run_minimal_range_fixtures.sh` compiles the real Task, Auth and
RangeDownloader sources with a fake transport and runs 130 assertions, including
a probe that fails if an event is delivered under the lock. URLSession behavior
was checked on the iOS 27 simulator against the real provider; physical-device
background timing still needs device runs.

## macOS and Windows

After the same probe, the app keeps a normal `DownloadTask` with pause and five
retries. It requests `Range: bytes=0-` with identity encoding and marks metadata
`rangeResume: true`, `rangeTotal: N`. A 206 activates the existing Dart resume
path even when Accept-Ranges is malformed. Both platforms use `DesktopDownloader`
and this patched Dart engine; Windows drive-letter and UNC destinations continue
through the plugin's existing path handling.

Marked responses require an exact 206/Content-Range start, end and total,
identity encoding, and Content-Length when present. The body cannot exceed the
remaining length, and the final file must contain exactly N bytes before
publication. Every retry asks the provider URL with the retained offset.
ResumeData consumed at admission is restored if a valid partial file survives an
error before body transfer, including HTTP 406 and transport errors. A valid
prefix is kept whatever the remaining retries, after a final failure and after a
404. A response with an invalid range keeps it only while retries remain; a
changed or weak ETag drops it at once, since the prefix belongs to another
version of the file. Cancellation and oversized bodies remove it. Existing ETag checks remain.
Without a validator, ranges and length are verified, not that the provider never
replaced the resource with different bytes of the same length.

Canceled queued marked resumes delete their retained prefix before the final
status clears ResumeData. All targets of a batch are removed from admission
before cleanup awaits. No cold-launch or process-termination guarantee is added.
A Windows app build and native runtime test remain unverified.

## Android

For ordinary file destinations, the same probe enables a normal `DownloadTask`
with the `rangeResume`/`rangeTotal` marker, identity encoding, pause and five
retries. Android URI destinations keep their upstream direct stream; they cannot
retain a temporary file for this recovery path.

The native runner requests the original task URL on every attempt, replacing
only the byte offset in Range. Marked responses require an exact 206,
Content-Range start/end/total, identity encoding and a matching Content-Length
when present. Body writes reject surplus bytes; publication requires the exact
probed total size. Existing strong ETag checks still apply on resume.

A valid partial file survives cuts, short bodies and errors before body
transfer, including HTTP 406, whatever the remaining retries, and also a final
failure or a 404, for a user retry. A response with an invalid range keeps it
only while retries remain; a changed or weak ETag drops it at once (upstream). The runner republishes the same offset and
input ETag after a rejected response without altering the file. Cancellation and
invalid oversized bodies delete owned partial files. A pause before any byte
stays a pause with an empty resume point (resuming starts at byte 0) instead of
a failure that would be retried automatically; cancellation during retry
backoff or after a final failure deletes the retained file in Dart, since no
native runner owns it then. WorkManager, foreground services and the native/Dart
message format are unchanged.

No additional guarantee is made for process termination, cache eviction or a cold
launch. The connected suite in `test/native/` runs the real Android plugin on an
emulator; physical-device timing remains unverified.

## Retry budget, retained failures and cancellation (shared Dart)

For marked `rangeResume` tasks, `BaseDownloader` remembers the retained offset
from each ResumeData. When a failure arrives after the offset advanced since the
previous failure, the full `retries` budget is restored before the upstream
retry logic runs, so long downloads survive any number of separate cuts. A final
`failed` or `notFound` of a range task (`rangeResume` or `rangeChunks`) keeps its
ResumeData, so `taskCanResume` is true and a resume continues from the kept
bytes; ResumeData whose prefix file the platform deleted is forgotten.
`rescheduleKilledTasks` resumes a range task that has ResumeData instead of
enqueuing it from byte 0. Cancelling such a failed Android/desktop task deletes its kept file in
Dart after the platform cancellation; paused or failed iOS range parents are
always cancelled by the platform, which owns their bytes. Unmarked tasks keep
the upstream behavior. `common/test/download/range_retry_budget_test.dart` in
the Flutter repository covers these rules.
