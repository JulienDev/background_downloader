// Run through run_android_range_fixtures.py in an isolated Android host.
// These tests use the real method-channel plugin and Android WorkManager.
// ignore_for_file: avoid_print

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:background_downloader/background_downloader.dart';
import 'package:crypto/crypto.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:path_provider/path_provider.dart';

const _total = 8 * 1024 * 1024;
const _cut = 3 * 1024 * 1024 + 37;
const _piece = 1024 * 1024;
const _group = 'android-range-fixtures';
const _timeout = Timeout(Duration(minutes: 2));
final _payload = Uint8List.fromList(
  List<int>.generate(
    _total,
    (index) => (index * 37 + (index >> 16) * 17 + 19) & 255,
  ),
);

late HttpServer _server;
late FileDownloader _downloader;
late IOSink _journal;
late StreamSubscription<TaskRecord> _databaseSubscription;
final _fixtures = <String, _Fixture>{};
final _tasks = <String, _ObservedTask>{};
var _sequence = 0;

void _log(String event, Map<String, Object?> values) {
  final line = jsonEncode({
    'time': DateTime.now().toUtc().toIso8601String(),
    'event': event,
    ...values,
  });
  // The file retains complete events even when Android logcat splits a line.
  _journal.writeln(line);
  print('ANDROID_RANGE_FIXTURE $line');
}

class _Fixture {
  _Fixture(this.name, this.mode);

  final String name;
  String mode;
  String? eTag;
  var token = 0;
  var bodyAttempts = 0;
  var bodyAllSent = false;
  // Media bodies whose last byte has not been queued yet: a client that waits
  // for each piece never sends the next request while one is open.
  var openBodies = 0;
  // Piece modes fail one offset once, then serve it normally.
  final failedOnce = <int>{};
  final origins = <Map<String, Object?>>[];
  final responses = <Map<String, Object?>>[];

  String get url => 'http://127.0.0.1:${_server.port}/original/$name';

  Future<void> handle(HttpRequest request) async {
    final headers = <String, List<String>>{};
    request.headers.forEach((name, values) => headers[name] = values);
    final range = request.headers.value(HttpHeaders.rangeHeader);
    final rangeMatch =
        range == null ? null : RegExp(r'^bytes=(\d+)-(\d*)').firstMatch(range);
    final start = rangeMatch == null ? 0 : int.parse(rangeMatch.group(1)!);
    // Bounded pieces ask for an end; the original requests are open-ended.
    final requestedEnd =
        rangeMatch == null || rangeMatch.group(2)!.isEmpty
            ? null
            : int.parse(rangeMatch.group(2)!);
    _log('httpRequest', {
      'fixture': name,
      'method': request.method,
      'url': 'http://127.0.0.1:${_server.port}${request.uri}',
      'headers': headers,
      'start': start,
    });
    if (request.method == 'HEAD') {
      request.response.headers.set('Accept-Ranges', '0-$_total');
      request.response.contentLength = 0;
      await request.response.close();
      return;
    }
    if (request.uri.path.startsWith('/original/')) {
      final origin = <String, Object?>{
        'range': range,
        'start': start,
        'end': requestedEnd,
        'token': ++token,
        'openBodies': openBodies,
      };
      origins.add(origin);
      request.response.statusCode = HttpStatus.found;
      request.response.headers.set(
        HttpHeaders.locationHeader,
        'http://127.0.0.1:${_server.port}/media/$name?token=$token',
      );
      request.response.contentLength = 0;
      _log('httpRedirect', {'fixture': name, ...origin});
      await request.response.close();
      return;
    }
    final probe = range == 'bytes=0-0';
    if (!probe) bodyAttempts++;
    final response = request.response;
    response.headers.set('Accept-Ranges', '0-$_total');
    response.headers.set(HttpHeaders.contentTypeHeader, 'video/x-matroska');
    var status = HttpStatus.partialContent;
    var rangeStart = start;
    // Last byte served: the requested piece, else the end of the file.
    var servedEnd =
        requestedEnd == null || requestedEnd >= _total
            ? _total - 1
            : requestedEnd;
    if (mode == 'pieceOpenEnded' && start > 0) {
      // A server that honors bytes=0-0 but answers later bounded ranges with
      // the rest of the file.
      servedEnd = _total - 1;
    }
    var rangeEnd = servedEnd;
    var rangeTotal = _total;
    var bodyEnd = servedEnd + 1;
    var chunked = false;
    var drop = false;
    var encoding = 'identity';
    var missingContentRange = false;

    if (probe) {
      rangeEnd = 0;
      bodyEnd = 1;
    } else {
      switch (mode) {
        case 'drop406':
          if (bodyAttempts == 1) {
            bodyEnd = _cut;
            drop = true;
          } else if (bodyAttempts == 2) {
            status = HttpStatus.notAcceptable;
          }
        case 'final406':
          if (bodyAttempts == 1) {
            bodyEnd = _cut;
            drop = true;
          } else {
            status = HttpStatus.notAcceptable;
          }
        case 'cutTwice':
          if (bodyAttempts == 1) {
            bodyEnd = _cut;
            drop = true;
          } else if (bodyAttempts == 2) {
            bodyEnd = 2 * _cut;
            drop = true;
          }
        case 'shortThenSuccess':
          chunked = true;
          if (bodyAttempts == 1) bodyEnd = _cut;
        case 'surplus':
          chunked = true;
          bodyEnd = _total + 8192;
        case 'status200':
          status = HttpStatus.ok;
        case 'rangeStart':
          rangeStart = start + 1;
        case 'rangeEnd':
          rangeEnd = _total - 2;
        case 'rangeTotal':
          rangeTotal = _total + 1;
        case 'missingRange':
          missingContentRange = true;
        case 'encoded':
          encoding = 'gzip';
        case 'duplicateRange':
        case 'duplicateEncoding':
        case 'changedETag':
        case 'weakETag':
          break;
        case 'holdEof':
          chunked = true;
        case 'badLength':
          break;
        case 'notFound':
          status = HttpStatus.notFound;
        case 'refuse':
          status = HttpStatus.notAcceptable;
        case 'valid':
        case 'pieceOpenEnded':
          break;
        case 'pieceCut406':
          // The third piece is cut once; its continuation is refused once
          // with an expired-token 406, as the provider does.
          if (start == 2 * _piece && failedOnce.add(start)) {
            bodyEnd = start + 300 * 1024 + 11;
            drop = true;
          } else if (start == 2 * _piece + 300 * 1024 + 11 &&
              failedOnce.add(start)) {
            status = HttpStatus.notAcceptable;
          }
        case 'pieceShort':
          // A valid chunked piece that ends early once.
          chunked = true;
          if (start == 3 * _piece && failedOnce.add(start)) {
            bodyEnd = start + 123457;
          }
        case 'pieceSurplus':
          chunked = true;
          if (start == 2 * _piece) bodyEnd = servedEnd + 1 + 8192;
        case 'piece404':
          if (start == 4 * _piece) status = HttpStatus.notFound;
        default:
          throw StateError('Unknown fixture mode $mode');
      }
    }
    response.statusCode = status;
    if (status >= 400) {
      final body = 'Synthetic $status: $name token=$token start=$start';
      response.contentLength = utf8.encode(body).length;
      response.write(body);
      final event = <String, Object?>{
        'fixture': name,
        'status': status,
        'start': start,
        'body': body,
      };
      responses.add(event);
      _log('httpResponse', event);
      await response.close();
      return;
    }
    if (!missingContentRange && status == HttpStatus.partialContent) {
      response.headers.set(
        'Content-Range',
        'bytes $rangeStart-$rangeEnd/$rangeTotal',
      );
    }
    response.headers.set('Content-Encoding', encoding);
    if (!probe && mode == 'duplicateRange') {
      response.headers.add(
        'Content-Range',
        'bytes $rangeStart-$rangeEnd/$rangeTotal',
      );
    }
    if (!probe && mode == 'duplicateEncoding') {
      response.headers.add('Content-Encoding', 'gzip');
    }
    final responseETag =
        !probe && mode == 'changedETag' ? '"changed-file"' : eTag;
    if (responseETag != null) response.headers.set('ETag', responseETag);
    response.contentLength =
        chunked
            ? -1
            : probe
            ? 1
            : servedEnd + 1 - start + (mode == 'badLength' ? 1 : 0);
    final responseHeaders = <String, List<String>>{};
    response.headers.forEach((name, values) => responseHeaders[name] = values);
    final event = <String, Object?>{
      'fixture': name,
      'status': status,
      'start': start,
      'headers': responseHeaders,
      'bodyEnd': bodyEnd,
      'drop': drop,
    };
    responses.add(event);
    _log('httpResponse', event);

    Socket? socket;
    var bodyOpen = !probe;
    if (bodyOpen) openBodies++;
    void bodyQueued() {
      if (bodyOpen) {
        bodyOpen = false;
        openBodies--;
      }
    }

    try {
      // The deliberate disconnect sends fewer bytes than Content-Length;
      // normal short/surplus EOF cases use valid chunked HTTP framing instead.
      if (drop) socket = await response.detachSocket(writeHeaders: true);
      for (var offset = start; offset < bodyEnd; offset += 32768) {
        final end = (offset + 32768).clamp(0, bodyEnd);
        final bytes =
            end <= _total
                ? Uint8List.sublistView(_payload, offset, end)
                : Uint8List.fromList([
                  if (offset < _total)
                    ...Uint8List.sublistView(_payload, offset),
                  ...List.filled(end - offset.clamp(0, _total), 211),
                ]);
        if (socket == null) {
          response.add(bytes);
          if (end == bodyEnd) bodyQueued();
          await response.flush();
        } else {
          socket.add(bytes);
          if (end == bodyEnd) bodyQueued();
          await socket.flush();
        }
        if (!probe) {
          await Future<void>.delayed(const Duration(milliseconds: 20));
        }
      }
      if (socket == null) {
        bodyAllSent = true;
        if (!probe && mode == 'holdEof') {
          _log('httpEofHeld', {'fixture': name, 'offset': bodyEnd});
          await Future<void>.delayed(const Duration(seconds: 5));
        }
        await response.close();
      } else {
        socket.destroy();
        _log('httpDisconnect', {'fixture': name, 'offset': bodyEnd});
      }
    } catch (error, stack) {
      // Cancellation and header validation close the native connection before
      // the synthetic server finishes writing. Retain the exact diagnostic.
      _log('httpWriteException', {
        'fixture': name,
        'error': '$error',
        'stack': '$stack',
      });
      bodyQueued();
      socket?.destroy();
      try {
        await response.close();
      } catch (_) {
        // The original write error above contains the complete diagnostic.
      }
    }
  }
}

class _ObservedTask {
  _ObservedTask(this.fixture, this.task);

  final _Fixture fixture;
  final DownloadTask task;
  final statuses = <TaskStatusUpdate>[];
  final progress = <TaskProgressUpdate>[];
  final records = <TaskRecord>[];

  Future<TaskStatusUpdate> waitStatus(TaskStatus status) async {
    await _until(
      () => statuses.any((update) => update.status == status),
      'Task ${task.taskId} never reached $status',
    );
    return statuses.lastWhere((update) => update.status == status);
  }

  Future<TaskStatusUpdate> waitFinal() async {
    await _until(
      () => statuses.any((update) => update.status.isFinalState),
      'Task ${task.taskId} never finished',
    );
    return statuses.lastWhere((update) => update.status.isFinalState);
  }

  Future<ResumeData> pause() async {
    await _until(
      () => progress.any((update) => update.progress >= 0.22),
      'Task ${task.taskId} did not make enough progress to pause',
    );
    expect(await _downloader.taskCanResume(task), isTrue);
    expect(await _downloader.pause(task), isTrue);
    await waitStatus(TaskStatus.paused);
    ResumeData? resume;
    await _until(() async {
      resume = await _downloader.downloaderForTesting.getResumeData(
        task.taskId,
      );
      return resume != null;
    }, 'No resume data for paused task ${task.taskId}');
    expect(resume!.requiredStartByte, greaterThan(1024 * 1024));
    expect(resume!.requiredStartByte, lessThan(_total));
    final prefix = await File(resume!.tempFilepath).readAsBytes();
    expect(prefix.length, resume!.requiredStartByte);
    expect(
      listEquals(prefix, Uint8List.sublistView(_payload, 0, prefix.length)),
      isTrue,
    );
    _log('pausedPrefix', {'fixture': fixture.name, ...resume!.toJson()});
    return resume!;
  }

  Future<void> expectPublished() async {
    expect((await waitFinal()).status, TaskStatus.complete);
    final file = File(await task.filePath());
    expect(await file.exists(), isTrue);
    final bytes = await file.readAsBytes();
    expect(bytes.length, _total);
    expect(listEquals(bytes, _payload), isTrue);
    _log('exactFile', {
      'fixture': fixture.name,
      'taskId': task.taskId,
      'path': file.path,
      'length': bytes.length,
      'sha256': sha256.convert(bytes).toString(),
      'expectedSha256': sha256.convert(_payload).toString(),
    });
    await expectClean();
  }

  Future<void> expectClean({bool unpublished = false}) async {
    if (unpublished) {
      expect(await File(await task.filePath()).exists(), isFalse);
    }
    await _until(() async {
      final resume = await _downloader.downloaderForTesting.getResumeData(
        task.taskId,
      );
      return resume == null && (await _nativePartials()).isEmpty;
    }, 'A finished task retained native resume data or a partial file');
    _log('cleanupVerified', {
      'fixture': fixture.name,
      'taskId': task.taskId,
      'unpublished': unpublished,
    });
  }
}

Future<void> _until(
  FutureOr<bool> Function() condition,
  String reason, {
  Duration timeout = const Duration(seconds: 70),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail(reason);
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}

Future<List<String>> _nativePartials() async {
  final found = <String>[];
  for (final directory in [
    await getTemporaryDirectory(),
    await getApplicationSupportDirectory(),
  ]) {
    await for (final entry in directory.list()) {
      if (entry is File &&
          entry.path
              .split('/')
              .last
              .startsWith('com.bbflight.background_downloader')) {
        found.add(entry.path);
      }
    }
  }
  return found;
}

Future<_ObservedTask> _create(
  String label, {
  String mode = 'valid',
  bool marked = true,
  int retries = 0,
  int? pieceBytes,
  String? eTag,
  List<int>? existingDestination,
}) async {
  final name = '${++_sequence}-$label';
  final fixture = _Fixture(name, mode);
  fixture.eTag = eTag;
  _fixtures[name] = fixture;
  final task = DownloadTask(
    taskId: 'fixture-$name',
    url: fixture.url,
    filename: '$name.mkv',
    directory: _group,
    baseDirectory: BaseDirectory.applicationDocuments,
    group: _group,
    updates: Updates.statusAndProgress,
    allowPause: true,
    retries: retries,
    headers:
        marked
            ? {'Range': 'bytes=0-', 'Accept-Encoding': 'identity'}
            : {'Accept-Encoding': 'gzip, deflate'},
    metaData:
        marked
            ? jsonEncode({
              'rangeResume': true,
              'rangeTotal': '$_total',
              if (pieceBytes != null) 'rangePieceBytes': pieceBytes,
            })
            : jsonEncode({'ordinaryTask': true}),
  );
  final observed = _ObservedTask(fixture, task);
  _tasks[task.taskId] = observed;
  _log('taskCreated', {'fixture': name, 'task': task.toJson()});
  if (existingDestination != null) {
    final file = File(await task.filePath());
    await file.parent.create(recursive: true);
    await file.writeAsBytes(existingDestination);
  }
  expect(await _downloader.enqueue(task), isTrue);
  return observed;
}

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  setUpAll(() async {
    expect(Platform.isAndroid, isTrue, reason: 'Requires real Android plugin');
    final support = await getApplicationSupportDirectory();
    _journal = File('${support.path}/android-range-fixtures.jsonl').openWrite();
    _log('start', {
      'platform': Platform.operatingSystemVersion,
      'journalPath': '${support.path}/android-range-fixtures.jsonl',
      'total': _total,
      'cut': _cut,
    });
    _server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((request) {
      final name = request.uri.pathSegments.last;
      final fixture = _fixtures[name];
      if (fixture == null) {
        request.response.statusCode = HttpStatus.notFound;
        unawaited(request.response.close());
        return;
      }
      unawaited(fixture.handle(request));
    });
    _downloader = FileDownloader();
    await _downloader.ready;
    await _downloader.configure(
      globalConfig: [(Config.holdingQueue, false)],
      androidConfig: [
        (Config.runInForeground, false),
        (Config.useCacheDir, Config.always),
        (Config.useExternalStorage, Config.never),
      ],
    );
    await _downloader.reset(group: _group);
    await _downloader.trackTasksInGroup(_group, markDownloadedComplete: false);
    _downloader.registerCallbacks(
      group: _group,
      taskStatusCallback: (update) {
        _tasks[update.task.taskId]?.statuses.add(update);
        _log('taskStatus', update.toJson());
      },
      taskProgressCallback: (update) {
        _tasks[update.task.taskId]?.progress.add(update);
        _log('taskProgress', update.toJson());
      },
    );
    _databaseSubscription = _downloader.database.updates.listen((record) {
      _tasks[record.taskId]?.records.add(record);
      _log('taskRecord', record.toJson());
    });
  });

  tearDown(() async {
    await _downloader.reset(group: _group);
    await _journal.flush();
    final destination = Directory(
      '${(await getApplicationDocumentsDirectory()).path}/$_group',
    );
    if (await destination.exists()) await destination.delete(recursive: true);
    expect(await _nativePartials(), isEmpty);
  });

  tearDownAll(() async {
    await _databaseSubscription.cancel();
    _downloader.unregisterCallbacks(group: _group);
    await _server.close(force: true);
    _log('finished', {'tasks': _tasks.length});
    await _journal.close();
  });

  testWidgets(
    'HEAD zero and malformed Accept-Ranges do not prevent a real 206',
    (tester) async {
      await tester.pumpWidget(
        const MaterialApp(home: Text('Android range tests')),
      );
      final fixture = _Fixture('probe', 'valid');
      _fixtures[fixture.name] = fixture;
      final client = HttpClient()..autoUncompress = false;
      try {
        final head = await client.headUrl(Uri.parse(fixture.url));
        final headResponse = await head.close();
        expect(headResponse.statusCode, HttpStatus.ok);
        expect(headResponse.contentLength, 0);
        expect(headResponse.headers.value('Accept-Ranges'), '0-$_total');
        await headResponse.drain<void>();
        final get = await client.getUrl(Uri.parse(fixture.url));
        get.headers.set('Range', 'bytes=0-0');
        get.headers.set('Accept-Encoding', 'identity');
        final response = await get.close();
        expect(response.statusCode, HttpStatus.partialContent);
        expect(response.headers.value('Content-Range'), 'bytes 0-0/$_total');
        final bytes = await response.fold<List<int>>(
          [],
          (all, part) => all..addAll(part),
        );
        expect(bytes, [_payload.first]);
        _log('probeVerified', {'url': fixture.url, 'total': _total});
      } finally {
        client.close(force: true);
      }
    },
    timeout: _timeout,
  );

  testWidgets(
    'cut then 406 resumes twice from the exact prefix and fresh origin',
    (tester) async {
      final observed = await _create('cut-406', mode: 'drop406', retries: 2);
      await observed.expectPublished();
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _cut, _cut],
      );
      expect(
        observed.fixture.origins.map((request) => request['token']).toList(),
        [1, 2, 3],
      );
      expect(
        observed.fixture.responses
            .map((response) => response['status'])
            .toList(),
        [206, 406, 206],
      );
      expect(
        observed.statuses
            .where((update) => update.status == TaskStatus.waitingToRetry)
            .length,
        2,
      );
      expect(observed.progress.any((update) => update.progress == -4), isTrue);
      var lastProgress = 0.0;
      for (final record in observed.records.where(
        (record) => record.progress >= 0,
      )) {
        expect(record.progress, greaterThanOrEqualTo(lastProgress));
        lastProgress = record.progress;
      }
      _log('offsetAndProgressVerified', {
        'taskId': observed.task.taskId,
        'offset': _cut,
      });
    },
    timeout: _timeout,
  );

  testWidgets('pause resumes the exact file through its original URL', (
    tester,
  ) async {
    final observed = await _create('pause');
    final resume = await observed.pause();
    expect(await _downloader.resume(observed.task), isTrue);
    await observed.expectPublished();
    expect(observed.fixture.origins.last['start'], resume.requiredStartByte);
    expect(observed.fixture.origins.length, 2);
  }, timeout: _timeout);

  for (final mode in [
    'status200',
    'rangeStart',
    'rangeEnd',
    'rangeTotal',
    'missingRange',
    'encoded',
    'badLength',
    'duplicateRange',
    'duplicateEncoding',
    'changedETag',
    'weakETag',
  ]) {
    testWidgets(
      'resumed $mode is rejected before publication and cleaned',
      (tester) async {
        final observed = await _create(
          'invalid-$mode',
          eTag:
              mode == 'changedETag'
                  ? '"stable-file"'
                  : mode == 'weakETag'
                  ? 'W/"stable-file"'
                  : null,
        );
        final resume = await observed.pause();
        observed.fixture.mode = mode;
        expect(await _downloader.resume(observed.task), isTrue);
        expect((await observed.waitFinal()).status, TaskStatus.failed);
        expect(
          observed.fixture.origins.last['start'],
          resume.requiredStartByte,
        );
        await observed.expectClean(unpublished: true);
      },
      timeout: _timeout,
    );
  }

  testWidgets(
    'valid chunked short EOF keeps a prefix and resumes exactly',
    (tester) async {
      final observed = await _create(
        'short-eof',
        mode: 'shortThenSuccess',
        retries: 1,
      );
      await observed.expectPublished();
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _cut],
      );
      expect(
        observed.fixture.responses.every(
          (response) =>
              !(response['headers'] as Map).containsKey('content-length'),
        ),
        isTrue,
      );
    },
    timeout: _timeout,
  );

  testWidgets(
    'a surplus chunked body is rejected and its partial is removed',
    (tester) async {
      final observed = await _create('surplus', mode: 'surplus');
      expect((await observed.waitFinal()).status, TaskStatus.failed);
      await observed.expectClean(unpublished: true);
    },
    timeout: _timeout,
  );

  testWidgets('cancellation during transfer removes the native partial', (
    tester,
  ) async {
    final observed = await _create('cancel-running');
    await _until(
      () => observed.progress.any((update) => update.progress > 0.22),
      'No progress before cancellation',
    );
    expect(await _downloader.cancel(observed.task), isTrue);
    expect((await observed.waitFinal()).status, TaskStatus.canceled);
    await observed.expectClean(unpublished: true);
  }, timeout: _timeout);

  testWidgets('cancellation while paused deletes the owned prefix', (
    tester,
  ) async {
    final observed = await _create('cancel-paused');
    final resume = await observed.pause();
    expect(await _downloader.cancel(observed.task), isTrue);
    expect((await observed.waitFinal()).status, TaskStatus.canceled);
    await observed.expectClean(unpublished: true);
    expect(await File(resume.tempFilepath).exists(), isFalse);
  }, timeout: _timeout);

  testWidgets(
    'cancellation during retry removes prefix and prevents another GET',
    (tester) async {
      final observed = await _create(
        'cancel-retry',
        mode: 'drop406',
        retries: 2,
      );
      await observed.waitStatus(TaskStatus.waitingToRetry);
      expect(await _downloader.cancel(observed.task), isTrue);
      expect((await observed.waitFinal()).status, TaskStatus.canceled);
      await observed.expectClean(unpublished: true);
      final requests = observed.fixture.origins.length;
      await Future<void>.delayed(const Duration(seconds: 3));
      expect(observed.fixture.origins.length, requests);
    },
    timeout: _timeout,
  );

  testWidgets('cancellation before EOF preserves an older final file', (
    tester,
  ) async {
    final existing = utf8.encode(
      'An older published file must survive cancellation.',
    );
    final observed = await _create(
      'cancel-at-eof',
      mode: 'holdEof',
      existingDestination: existing,
    );
    await _until(
      () => observed.fixture.bodyAllSent,
      'The complete body was not sent before cancellation',
    );
    expect(await _downloader.cancel(observed.task), isTrue);
    expect((await observed.waitFinal()).status, TaskStatus.canceled);
    await observed.expectClean();
    expect(await File(await observed.task.filePath()).readAsBytes(), existing);
  }, timeout: _timeout);

  testWidgets(
    'batch cancellation cleans both retries and prevents both GETs',
    (tester) async {
      final first = await _create(
        'batch-cancel-a',
        mode: 'drop406',
        retries: 2,
      );
      final second = await _create(
        'batch-cancel-b',
        mode: 'drop406',
        retries: 2,
      );
      await _until(
        () =>
            first.statuses.isNotEmpty &&
            second.statuses.isNotEmpty &&
            first.statuses.last.status == TaskStatus.waitingToRetry &&
            second.statuses.last.status == TaskStatus.waitingToRetry,
        'Both tasks never waited for retry together',
      );
      expect(
        await _downloader.cancelTasksWithIds([
          first.task.taskId,
          second.task.taskId,
        ]),
        isTrue,
      );
      expect((await first.waitFinal()).status, TaskStatus.canceled);
      expect((await second.waitFinal()).status, TaskStatus.canceled);
      await first.expectClean(unpublished: true);
      await second.expectClean(unpublished: true);
      final firstRequests = first.fixture.origins.length;
      final secondRequests = second.fixture.origins.length;
      await Future<void>.delayed(const Duration(seconds: 3));
      expect(first.fixture.origins.length, firstRequests);
      expect(second.fixture.origins.length, secondRequests);
      _log('batchCancellationVerified', {
        'taskIds': [first.task.taskId, second.task.taskId],
        'originRequests': [firstRequests, secondRequests],
      });
    },
    timeout: _timeout,
  );

  testWidgets(
    'a cut on the last allowed retry after progress refills the budget',
    (tester) async {
      final observed = await _create('cut-twice', mode: 'cutTwice', retries: 1);
      await observed.expectPublished();
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _cut, 2 * _cut],
      );
    },
    timeout: _timeout,
  );

  testWidgets(
    '406 after the final allowed retry keeps the prefix for a user retry',
    (tester) async {
      final observed = await _create('final-406', mode: 'final406', retries: 1);
      final finalUpdate = await observed.waitFinal();
      expect(finalUpdate.status, TaskStatus.failed);
      expect(finalUpdate.exception, isA<TaskHttpException>());
      expect(
        (finalUpdate.exception! as TaskHttpException).httpResponseCode,
        HttpStatus.notAcceptable,
      );
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _cut],
      );
      final kept = await _downloader.downloaderForTesting.getResumeData(
        observed.task.taskId,
      );
      expect(kept?.requiredStartByte, _cut);
      expect(await File(kept!.tempFilepath).length(), _cut);
      expect(await _downloader.taskCanResume(observed.task), isTrue);
      expect(await _downloader.cancelTaskWithId(observed.task.taskId), isTrue);
      await observed.expectClean(unpublished: true);
    },
    timeout: _timeout,
  );

  testWidgets(
    'a user retry after a final failure continues from the kept prefix',
    (tester) async {
      final observed = await _create('retry-406', mode: 'final406', retries: 1);
      expect((await observed.waitFinal()).status, TaskStatus.failed);
      observed.fixture.mode = 'valid';
      observed.statuses.clear(); // wait for the retry's own final status
      expect(await _downloader.resume(observed.task), isTrue);
      await observed.expectPublished();
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _cut, _cut],
      );
    },
    timeout: _timeout,
  );

  testWidgets('404 during resume keeps the prefix until canceled', (
    tester,
  ) async {
    final observed = await _create('resume-404');
    final resume = await observed.pause();
    observed.fixture.mode = 'notFound';
    expect(await _downloader.resume(observed.task), isTrue);
    expect((await observed.waitFinal()).status, TaskStatus.notFound);
    expect(await File(resume.tempFilepath).exists(), isTrue);
    expect(
      (await _downloader.downloaderForTesting.getResumeData(
        observed.task.taskId,
      ))?.requiredStartByte,
      resume.requiredStartByte,
    );
    expect(await _downloader.cancelTaskWithId(observed.task.taskId), isTrue);
    await observed.expectClean(unpublished: true);
    expect(await File(resume.tempFilepath).exists(), isFalse);
  }, timeout: _timeout);

  testWidgets(
    'pieces request bounded ranges one after another, each from the origin',
    (tester) async {
      final observed = await _create('pieces', pieceBytes: _piece);
      await observed.expectPublished();
      final origins = observed.fixture.origins;
      expect(origins.map((request) => request['start']).toList(), [
        for (var start = 0; start < _total; start += _piece) start,
      ]);
      expect(origins.map((request) => request['end']).toList(), [
        for (var start = 0; start < _total; start += _piece) start + _piece - 1,
      ]);
      // A fresh redirect per piece, and never two bodies at once.
      expect(origins.map((request) => request['token']).toList(), [
        for (var token = 1; token <= _total ~/ _piece; token++) token,
      ]);
      expect(origins.every((request) => request['openBodies'] == 0), isTrue);
      expect(
        observed.fixture.responses.every(
          (response) => response['status'] == HttpStatus.partialContent,
        ),
        isTrue,
      );
      expect(
        observed.statuses.any(
          (update) => update.status == TaskStatus.waitingToRetry,
        ),
        isFalse,
      );
      var lastProgress = 0.0;
      for (final update in observed.progress.where(
        (update) => update.progress >= 0,
      )) {
        expect(update.progress, greaterThanOrEqualTo(lastProgress));
        lastProgress = update.progress;
      }
      _log('piecesVerified', {
        'taskId': observed.task.taskId,
        'pieces': origins.length,
      });
    },
    timeout: _timeout,
  );

  testWidgets(
    'a cut piece then 406 resumes from the exact byte with bounded pieces',
    (tester) async {
      final observed = await _create(
        'piece-cut-406',
        mode: 'pieceCut406',
        retries: 2,
        pieceBytes: _piece,
      );
      await observed.expectPublished();
      const cut = 2 * _piece + 300 * 1024 + 11;
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [
          0,
          _piece,
          2 * _piece,
          cut,
          for (var start = cut; start < _total; start += _piece) start,
        ],
      );
      expect(
        observed.fixture.origins.map((request) => request['end']).toList(),
        [
          _piece - 1,
          2 * _piece - 1,
          3 * _piece - 1,
          cut + _piece - 1,
          for (var start = cut; start < _total; start += _piece)
            (start + _piece).clamp(0, _total) - 1,
        ],
      );
      expect(
        observed.fixture.origins.every((request) => request['openBodies'] == 0),
        isTrue,
      );
      expect(
        observed.statuses
            .where((update) => update.status == TaskStatus.waitingToRetry)
            .length,
        2,
      );
    },
    timeout: _timeout,
  );

  testWidgets(
    'a short chunked piece keeps its bytes and resumes after them',
    (tester) async {
      final observed = await _create(
        'piece-short',
        mode: 'pieceShort',
        retries: 1,
        pieceBytes: _piece,
      );
      await observed.expectPublished();
      const short = 3 * _piece + 123457;
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [
          0,
          _piece,
          2 * _piece,
          3 * _piece,
          for (var start = short; start < _total; start += _piece) start,
        ],
      );
    },
    timeout: _timeout,
  );

  testWidgets(
    'a piece answered with the rest of the file is rejected, then dropped on the last retry',
    (tester) async {
      final observed = await _create(
        'piece-open-ended',
        mode: 'pieceOpenEnded',
        retries: 1,
        pieceBytes: _piece,
      );
      final finalUpdate = await observed.waitFinal();
      expect(finalUpdate.status, TaskStatus.failed);
      expect(
        observed.fixture.origins.map((request) => request['start']).toList(),
        [0, _piece, _piece],
      );
      await observed.expectClean(unpublished: true);
    },
    timeout: _timeout,
  );

  testWidgets('a surplus piece body is rejected and its partial removed', (
    tester,
  ) async {
    final observed = await _create(
      'piece-surplus',
      mode: 'pieceSurplus',
      pieceBytes: _piece,
    );
    expect((await observed.waitFinal()).status, TaskStatus.failed);
    expect(
      observed.fixture.origins.map((request) => request['start']).toList(),
      [0, _piece, 2 * _piece],
    );
    await observed.expectClean(unpublished: true);
  }, timeout: _timeout);

  testWidgets('a 404 piece keeps the earlier pieces until canceled', (
    tester,
  ) async {
    final observed = await _create(
      'piece-404',
      mode: 'piece404',
      pieceBytes: _piece,
    );
    expect((await observed.waitFinal()).status, TaskStatus.notFound);
    final kept = await _downloader.downloaderForTesting.getResumeData(
      observed.task.taskId,
    );
    expect(kept?.requiredStartByte, 4 * _piece);
    final prefix = await File(kept!.tempFilepath).readAsBytes();
    expect(
      listEquals(prefix, Uint8List.sublistView(_payload, 0, 4 * _piece)),
      isTrue,
    );
    expect(await _downloader.taskCanResume(observed.task), isTrue);
    expect(await _downloader.cancelTaskWithId(observed.task.taskId), isTrue);
    await observed.expectClean(unpublished: true);
    expect(await File(kept.tempFilepath).exists(), isFalse);
  }, timeout: _timeout);

  testWidgets('pause during pieces resumes with a bounded piece', (
    tester,
  ) async {
    final observed = await _create('piece-pause', pieceBytes: _piece);
    final resume = await observed.pause();
    final paused = observed.fixture.origins.length;
    expect(await _downloader.resume(observed.task), isTrue);
    await observed.expectPublished();
    final resumed = observed.fixture.origins[paused];
    expect(resumed['start'], resume.requiredStartByte);
    expect(resumed['end'], resume.requiredStartByte + _piece - 1);
    expect(
      observed.fixture.origins
          .take(paused)
          .every((request) => request['openBodies'] == 0),
      isTrue,
    );
    // This server never sees the paused piece's body end: a flush to the
    // connection the client closed never completes. The resumed pieces still
    // never overlap: none finds more open bodies than the first.
    final after = observed.fixture.origins.skip(paused);
    expect(
      after.every((request) => request['openBodies'] == resumed['openBodies']),
      isTrue,
    );
  }, timeout: _timeout);

  testWidgets('cancellation during pieces removes the native partial', (
    tester,
  ) async {
    final observed = await _create('piece-cancel', pieceBytes: _piece);
    await _until(
      () => observed.progress.any((update) => update.progress > 0.3),
      'No progress before cancellation',
    );
    expect(await _downloader.cancel(observed.task), isTrue);
    expect((await observed.waitFinal()).status, TaskStatus.canceled);
    await observed.expectClean(unpublished: true);
    final requests = observed.fixture.origins.length;
    await Future<void>.delayed(const Duration(seconds: 2));
    expect(observed.fixture.origins.length, requests);
  }, timeout: _timeout);

  testWidgets(
    'unmarked HTTP 200 retains the existing ordinary download path',
    (tester) async {
      final observed = await _create(
        'unmarked',
        mode: 'status200',
        marked: false,
      );
      await observed.expectPublished();
      expect(observed.fixture.origins.single['range'], isNull);
      expect(observed.fixture.responses.single['status'], HttpStatus.ok);
      expect(
        observed.statuses.any(
          (update) => update.status == TaskStatus.waitingToRetry,
        ),
        isFalse,
      );
    },
    timeout: _timeout,
  );
}
