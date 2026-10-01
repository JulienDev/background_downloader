import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

import '../exceptions.dart';
import '../models.dart';
import '../task.dart';
import '../utils.dart';
import 'desktop_downloader.dart';
import 'isolate.dart';

var taskRangeStartByte = 0; // Start of the Task's download range
String? eTagHeader;
late DownloadTask downloadTask; // global because filename may change

/// Only app-probed desktop tasks opt into strict, original-URL byte recovery.
int? rangeResumeTotal(DownloadTask task) {
  dynamic metadata;
  try {
    metadata = jsonDecode(task.metaData);
  } on FormatException {
    // Existing task metadata is not required to be JSON.
    return null;
  }
  if (metadata is! Map || metadata['rangeResume'] != true) return null;
  final total = int.tryParse('${metadata['rangeTotal']}');
  if (total == null || total <= 0) {
    throw TaskResumeException(
      'Invalid rangeResume total: ${metadata['rangeTotal']}',
    );
  }
  return total;
}

void validateRangeResumeResponse(
  http.StreamedResponse response,
  int start,
  int total,
) {
  final range = response.headers['content-range'];
  final encoding = response.headers['content-encoding'];
  final length = response.headers['content-length'];
  if (response.statusCode != 206 ||
      range?.trim().toLowerCase() != 'bytes $start-${total - 1}/$total' ||
      (encoding != null && encoding.trim().toLowerCase() != 'identity') ||
      (length != null && int.tryParse(length) != total - start)) {
    throw TaskResumeException(
      'Invalid range resume response: status=${response.statusCode}, '
      'headers=${response.headers}, expected=bytes $start-${total - 1}/$total',
    );
  }
}

/// Execute the download task
///
/// Sends updates via the [sendPort] and can be commanded to cancel/pause via
/// the [messagesToIsolate] queue
Future<void> doDownloadTask(
  DownloadTask task,
  ResumeData? resumeData,
  bool isResume,
  Duration requestTimeout,
  SendPort sendPort,
) async {
  // use downloadTask from here on as a 'global' variable in this isolate,
  // as we may change the filename of the task
  downloadTask = task;
  var filePath = await downloadTask.filePath();
  // tempFilePath is taken from [resumeDataString] if this is a resuming task.
  // Otherwise, it is a generated full path to the temp directory
  final tempFilePath =
      isResume && resumeData != null
          ? resumeData.tempFilepath
          : p.join(
            (await getTemporaryDirectory()).path,
            'com.bbflight.background_downloader${Random().nextInt(1 << 32).toString()}',
          );
  final requiredStartByte =
      resumeData?.requiredStartByte ?? 0; // start for resume
  final eTag = resumeData?.eTag;
  isResume =
      isResume &&
      await determineIfResumeIsPossible(tempFilePath, requiredStartByte);
  final rangeTotal = rangeResumeTotal(downloadTask);
  final validResume = isResume;
  var processedBody = false;
  // The server answered with a range or identity contradicting the prefix.
  var rejectedRangeResponse = false;
  // A changed or weak ETag: the prefix belongs to another version of the file.
  var eTagChanged = false;
  final client = DesktopDownloader.httpClient;
  var request = http.Request(
    downloadTask.httpRequestMethod,
    Uri.parse(downloadTask.url),
  );
  request.headers.addAll(downloadTask.headers);
  request.persistentConnection = false;
  if (isResume) {
    final taskRangeHeader = downloadTask.headers['Range'] ?? '';
    final taskRange = parseRange(taskRangeHeader);
    taskRangeStartByte = taskRange.$1;
    final resumeRange = (taskRangeStartByte + requiredStartByte, taskRange.$2);
    final newRangeString = 'bytes=${resumeRange.$1}-${resumeRange.$2 ?? ""}';
    request.headers['Range'] = newRangeString;
  }
  if (downloadTask.post case String post) {
    request.body = post;
  }
  var resultStatus = TaskStatus.failed;
  try {
    if (rangeTotal != null && requiredStartByte >= rangeTotal) {
      throw TaskResumeException(
        'Resume offset $requiredStartByte >= $rangeTotal',
      );
    }
    if (rangeTotal != null) {
      log.fine(
        'Range download request ${request.url} headers=${request.headers}',
      );
    }
    final response = await client.send(request);
    if (!isCanceled) {
      if (rangeTotal != null) {
        log.fine(
          'Range download response ${request.url} status=${response.statusCode} '
          'headers=${response.headers}',
        );
        // Preserve HTTP errors (including their body) before range validation.
        if (okResponses.contains(response.statusCode)) {
          try {
            validateRangeResumeResponse(
              response,
              validResume ? requiredStartByte : 0,
              rangeTotal,
            );
          } catch (_) {
            rejectedRangeResponse = true;
            rethrow;
          }
        }
      }
      eTagHeader = response.headers['etag'] ?? response.headers['ETag'];
      final acceptRangesHeader = response.headers['accept-ranges'];
      final serverAcceptsRanges =
          acceptRangesHeader == 'bytes' || response.statusCode == 206;
      var taskCanResume = false;
      if (downloadTask.allowPause) {
        // determine if this task can be paused
        taskCanResume = serverAcceptsRanges;
        sendPort.send(('taskCanResume', taskCanResume));
      }
      isResume =
          isResume && response.statusCode == 206; // confirm resume response
      if (isResume && (eTagHeader != eTag || eTag?.startsWith('W/') == true)) {
        eTagChanged = true;
        throw TaskException('Cannot resume: ETag is not identical, or is weak');
      }
      if (!downloadTask.hasFilename) {
        downloadTask = await taskWithSuggestedFilename(
          downloadTask,
          response.headers,
          true,
        );
        // update the filePath by replacing the last segment with the new filename
        filePath = p.join(p.dirname(filePath), downloadTask.filename);
        log.finest(
          'Suggested filename for taskId ${task.taskId}: ${task.filename}',
        );
      }
      responseHeaders = response.headers;
      responseStatusCode = response.statusCode;
      extractContentType(response.headers);
      if (okResponses.contains(response.statusCode)) {
        processedBody = true;
        resultStatus = await processOkDownloadResponse(
          filePath,
          tempFilePath,
          serverAcceptsRanges,
          taskCanResume,
          isResume,
          requestTimeout,
          response,
          sendPort,
          rangeTotal: rangeTotal,
          expectedBodyLength:
              rangeTotal == null
                  ? null
                  : rangeTotal - (validResume ? requiredStartByte : 0),
        );
      } else {
        // not an OK response
        responseBody = await responseContent(response);
        if (response.statusCode == 404) {
          resultStatus = TaskStatus.notFound;
        } else {
          taskException = TaskHttpException(
            responseBody?.isNotEmpty == true
                ? responseBody!
                : response.reasonPhrase ?? 'Invalid HTTP Request',
            response.statusCode,
          );
        }
      }
    }
  } catch (e) {
    logError(downloadTask, e.toString());
    setTaskError(e);
  }
  if (isCanceled) {
    // cancellation overrides other results
    resultStatus = TaskStatus.canceled;
  }
  if (rangeTotal != null && validResume && !processedBody) {
    // Desktop consumes ResumeData before starting an attempt. Restore it when
    // an expired token or transport error prevented any bytes from being read.
    // A marked prefix is kept even when no retry remains: Dart keeps its
    // ResumeData after a final failure so a user retry continues from it. A
    // final response contradicting the prefix drops it.
    final file = File(tempFilePath);
    if ((resultStatus == TaskStatus.failed ||
            resultStatus == TaskStatus.notFound) &&
        !eTagChanged &&
        !(rejectedRangeResponse && downloadTask.retriesRemaining <= 0) &&
        requiredStartByte > 0 &&
        requiredStartByte < rangeTotal &&
        await file.exists() &&
        await file.length() == requiredStartByte) {
      sendPort.send(('resumeData', tempFilePath, requiredStartByte, eTag));
    } else {
      deleteTempFile(tempFilePath);
    }
  }
  processStatusUpdateInIsolate(downloadTask, resultStatus, sendPort);
}

/// Return true if resume is possible
///
/// Confirms that file at [tempFilePath] exists and its length equals
/// [requiredStartByte]
Future<bool> determineIfResumeIsPossible(
  String tempFilePath,
  int requiredStartByte,
) async {
  if (File(tempFilePath).existsSync()) {
    if (await File(tempFilePath).length() == requiredStartByte) {
      return true;
    } else {
      log.fine('Partially downloaded file is corrupted, resume not possible');
    }
  } else {
    log.fine('Partially downloaded file not available, resume not possible');
  }
  return false;
}

/// Process response with valid response code
///
/// Performs the actual bytes transfer from response to a temp file,
/// and handles the result of the transfer:
/// - .complete -> copy temp to final file location
/// - .failed -> delete temp file
/// - .paused -> post resume information
Future<TaskStatus> processOkDownloadResponse(
  String filePath,
  String tempFilePath,
  bool serverAcceptsRanges,
  bool taskCanResume,
  bool isResume,
  Duration requestTimeout,
  http.StreamedResponse response,
  SendPort sendPort, {
  int? rangeTotal,
  int? expectedBodyLength,
}) async {
  // contentLength is extracted from response header, and if not available
  // we attempt to extract from [Task.headers], allowing developer to
  // set the content length if already known
  final contentLength =
      expectedBodyLength ?? getContentLength(response.headers, downloadTask);
  isResume = isResume && response.statusCode == 206;
  if (isResume && !await prepareResume(response, tempFilePath)) {
    deleteTempFile(tempFilePath);
    return TaskStatus.failed;
  }
  var resultStatus = TaskStatus.failed;
  var actualTempFilePath = tempFilePath;
  var invalidRangeBody = false;
  IOSink? outStream;
  try {
    // do the actual download
    try {
      Directory(p.dirname(actualTempFilePath)).createSync(recursive: true);
      outStream = File(
        actualTempFilePath,
      ).openWrite(mode: isResume ? FileMode.append : FileMode.write);
    } catch (e) {
      if (!isResume) {
        // Fallback to the target download directory as a hidden file
        final targetDir = p.dirname(filePath);
        Directory(targetDir).createSync(recursive: true);
        actualTempFilePath = p.join(targetDir, '.${p.basename(tempFilePath)}');
        log.info(
          'Standard temporary directory not writeable ($e). '
          'Falling back to target directory temp file: $actualTempFilePath',
        );
        outStream = File(actualTempFilePath).openWrite(mode: FileMode.write);
      } else {
        rethrow;
      }
    }
    Stream<List<int>> inStream = response.stream;
    if (expectedBodyLength != null) {
      var received = 0;
      inStream = inStream.map((bytes) {
        received += bytes.length;
        if (received > expectedBodyLength) {
          invalidRangeBody = true;
          throw TaskResumeException(
            'Range body too long: received $received, expected $expectedBodyLength',
          );
        }
        return bytes;
      });
    }
    final transferBytesResult = await transferBytes(
      inStream,
      outStream,
      contentLength,
      downloadTask,
      sendPort,
      requestTimeout,
    );
    switch (transferBytesResult) {
      case TaskStatus.complete:
        // copy file to destination, creating dirs if needed
        await outStream.flush();
        if (rangeTotal != null &&
            await File(actualTempFilePath).length() != rangeTotal) {
          throw TaskResumeException(
            'Incomplete range download: ${await File(actualTempFilePath).length()} '
            'bytes, expected $rangeTotal',
          );
        }
        final dirPath = p.dirname(filePath);
        Directory(dirPath).createSync(recursive: true);
        File(actualTempFilePath).copySync(filePath);
        resultStatus = TaskStatus.complete;

      case TaskStatus.canceled:
        deleteTempFile(actualTempFilePath);
        resultStatus = TaskStatus.canceled;

      case TaskStatus.paused:
        if (taskCanResume) {
          sendPort.send((
            'resumeData',
            actualTempFilePath,
            bytesTotal + startByte,
            eTagHeader,
          ));
          resultStatus = TaskStatus.paused;
        } else {
          taskException = TaskResumeException(
            'Task was paused but cannot resume',
          );
          resultStatus = TaskStatus.failed;
        }

      case TaskStatus.failed:
        break;

      default:
        throw ArgumentError('Cannot process $transferBytesResult');
    }
  } catch (e) {
    logError(downloadTask, e.toString());
    setTaskError(e);
  } finally {
    try {
      try {
        await outStream?.close();
      } catch (_) {}
      final partialLength =
          rangeTotal != null && await File(actualTempFilePath).exists()
              ? await File(actualTempFilePath).length()
              : bytesTotal + startByte;
      if (resultStatus == TaskStatus.failed &&
          serverAcceptsRanges &&
          (rangeTotal == null
              ? partialLength > 1 << 20
              // Marked tasks keep any valid prefix, whatever the remaining
              // retries: Dart refills the budget after progress and keeps the
              // ResumeData after a final failure.
              : !invalidRangeBody &&
                    partialLength > 0 &&
                    partialLength < rangeTotal)) {
        // send ResumeData to allow resume after fail
        sendPort.send((
          'resumeData',
          actualTempFilePath,
          partialLength,
          eTagHeader,
        ));
      } else if (resultStatus != TaskStatus.paused) {
        File(actualTempFilePath).deleteSync();
      }
    } catch (e) {
      logError(
        downloadTask,
        'Could not delete temp file $actualTempFilePath: $e',
      );
    }
  }
  return resultStatus;
}

/// Prepare for resume if possible
///
/// Returns true if task can continue, false if task failed.
/// Extracts and parses Range headers, and truncates temp file
Future<bool> prepareResume(
  http.StreamedResponse response,
  String tempFilePath,
) async {
  final range = response.headers['content-range'];
  if (range == null) {
    log.fine('Could not process partial response Content-Range');
    taskException = TaskResumeException(
      'Could not process partial response Content-Range',
    );
    return false;
  }
  final contentRangeRegEx = RegExp(r"(\d+)-(\d+)/(\d+)");
  final matchResult = contentRangeRegEx.firstMatch(range);
  if (matchResult == null) {
    log.fine('Could not process partial response Content-Range $range');
    taskException = TaskResumeException(
      'Could not process '
      'partial response Content-Range $range',
    );
    return false;
  }
  final start = int.parse(matchResult.group(1) ?? '0');
  final end = int.parse(matchResult.group(2) ?? '0');
  final total = int.parse(matchResult.group(3) ?? '0');
  final tempFile = File(tempFilePath);
  final tempFileLength = await tempFile.length();
  log.finest(
    'Resume start=$start, end=$end of total=$total bytes, tempFile = $tempFileLength bytes',
  );
  startByte = start - taskRangeStartByte; // relative to start of range
  if (startByte > tempFileLength) {
    log.fine('Offered range not feasible: $range with startByte $startByte');
    taskException = TaskResumeException(
      'Offered range not feasible: $range with startByte $startByte',
    );
    return false;
  }
  try {
    final file = await tempFile.open(mode: FileMode.writeOnlyAppend);
    await file.truncate(startByte);
    file.close();
  } on FileSystemException {
    log.fine('Could not truncate temp file');
    taskException = TaskResumeException('Could not truncate temp file');
    return false;
  }
  return true;
}

/// Delete the temporary file
void deleteTempFile(String tempFilePath) async {
  try {
    File(tempFilePath).deleteSync();
  } on FileSystemException {
    log.fine('Could not delete temp file $tempFilePath');
  }
}

/// Extract content type from [headers] and set [mimeType] and [charSet]
void extractContentType(Map<String, String> headers) {
  final contentType = headers['content-type'];
  if (contentType != null) {
    final regEx = RegExp(r'(.*);\s*charset\s*=(.*)');
    final match = regEx.firstMatch(contentType);
    if (match != null) {
      mimeType = match.group(1);
      charSet = match.group(2);
    } else {
      mimeType = contentType;
    }
  }
}
