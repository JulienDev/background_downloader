# Native range fixtures

## iOS

Run `bash test/native/run_minimal_range_fixtures.sh` on macOS with Xcode installed.
The runner compiles the real Task, Auth and RangeDownloader sources with a
test-only bridge, a fake transport and temporary files. It performs no network
requests.

The 130 assertions cover markers and piece planning, sequential exact output,
406, ignored-range and transport retries with session-side backoff, session
resumed pieces, failures that keep their bytes (exhausted budget, 404, refused
piece) and their retry, pause/resume, cancellation, group cancellation, untracked
cleanup, WiFi policy restarts, legacy state decoding, restoration after termination (live and lost
pieces, interrupted and short appends), bounded background pieces, halving after failures, download generations, the moved first piece, interrupted publication, orphaned partial cleanup, foreground
promotion of idle background pieces, safe publication and event delivery outside
the controller's lock. The URLSession transport, notifications and device
background timing require simulator or iPhone integration runs.

## Android

From this package, use an explicitly selected disposable Android device or
emulator and the workspace's configured Flutter 3.41.9/JDK 21:

```sh
python3 test/native/run_android_range_fixtures.py --device emulator-5580 --workspace <iptv-flutter checkout>
```

The runner creates a separate Flutter host with the local plugin dependency and
the workspace's final APK native gate. It verifies the packaged native bytes and
APK signature before Flutter installs the test host. It never chooses or starts
an emulator, changes an AVD profile, or contacts a playlist provider. Use
`--prepare-only` to create and analyze the host without building or installing;
`--prepared-host <path>` reuses that host for a later run. Each evidence run
should have its own `--artifacts <path>` directory.

The connected fixtures exercise the actual Kotlin DownloadTaskRunner through
FileDownloader/WorkManager against an in-app HTTP server. They cover malformed
HEAD/Accept-Ranges, a cut followed by HTTP 406, original-URL redirects, retained
offsets and progress, exact output bytes, pause/resume, rejected statuses/ranges/
encoding/length, short and surplus bodies, ETag changes, cancellation, a cut on
the last retry after progress (budget refill), prefixes kept after a final 406 or
a 404 and removed by cancel, a user retry continuing from the kept prefix, and an
unmarked control. Journals, logcat, source fingerprints and
APK proofs are retained. The exact publication/cancellation race is also guarded
in native source; these fixtures do not inject a production-only timing hook.
Emulator tests do not prove physical-device background timing, process death or
cache-eviction recovery.
