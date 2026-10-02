package com.bbflight.background_downloader

import android.content.Context
import android.net.Uri
import android.os.Build
import android.os.storage.StorageManager
import android.util.Log
import androidx.core.net.toFile
import androidx.documentfile.provider.DocumentFile
import androidx.preference.PreferenceManager
import com.bbflight.background_downloader.UriUtils.unpack
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.BufferedInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.FilterOutputStream
import java.io.IOException
import java.io.RandomAccessFile
import java.net.HttpURLConnection
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import android.system.Os
import android.system.ErrnoException
import kotlin.math.absoluteValue
import kotlin.random.Random

class DownloadTaskRunner(context: TaskJobContext) : TaskRunner(context) {

    private var eTagHeader: String? = null
    private var serverAcceptsRanges = false // if true, send resume data on fail
    private var tempFilePath = ""
    private var requiredStartByte = 0L // required start byte within the task range
    private var taskRangeStartByte = 0L // Start of the Task's download range
    private var eTag: String? = null
    private var rangeBodyStarted = false
    private var invalidRangeBody = false
    // The server answered with a range or identity contradicting the prefix.
    private var rejectedRangeResponse = false
    private var publishedRangeFile: File? = null

    // Only app-probed, full-file ranges opt into this local recovery path.
    private val rangeTotal: Long? by lazy {
        val metadata = try {
            Json.parseToJsonElement(task.metaData) as? JsonObject
        } catch (_: Exception) {
            null
        }
        if (metadata?.get("rangeResume") != JsonPrimitive(true)) null
        else {
            val total = (metadata["rangeTotal"] as? JsonPrimitive)?.content?.toLongOrNull()
            if (total == null || total <= 0) throw IOException("Invalid rangeTotal: ${task.metaData}")
            total
        }
    }

    /**
     * Make the request to the [connection] and process the [Task]
     *
     * Returns the [TaskStatus]
     * */
    override suspend fun connectAndProcess(connection: HttpURLConnection): TaskStatus {
        if (rangeTotal != null) {
            connection.setRequestProperty("Range", "bytes=${if (isResume) requiredStartByte else 0}-")
            connection.setRequestProperty("Accept-Encoding", "identity")
            Log.i(TAG, "Range request ${task.taskId}: ${task.url}, headers=${connection.requestProperties}")
        } else if (isResume) {
            val taskRangeHeader = task.headers["Range"] ?: ""
            val taskRange = parseRange(taskRangeHeader)
            taskRangeStartByte = taskRange.first
            val resumeRange = Pair(taskRangeStartByte + requiredStartByte, taskRange.second)
            val newRangeString = "bytes=${resumeRange.first}-${resumeRange.second ?: ""}"
            connection.setRequestProperty("Range", newRangeString)
        }
        try {
            var result = super.connectAndProcess(connection)
            if (rangeTotal != null && result == TaskStatus.paused && !rangeBodyStarted) {
                BDPlugin.pausedTaskIds.remove(task.taskId)
                if (isResume && File(tempFilePath).length() == requiredStartByte) {
                    processResumeData(ResumeData(task, tempFilePath, requiredStartByte, eTag), prefs)
                } else {
                    // Paused before any byte: stay paused with an empty resume
                    // point (a failure here would be retried automatically).
                    // Resuming finds no prefix and starts at byte 0.
                    Log.i(TAG, "Paused range task ${task.taskId} before any data; resume starts at 0")
                    processResumeData(ResumeData(task, "", 0, null), prefs)
                }
            }
            if (result == TaskStatus.canceled ||
                (rangeTotal != null && BDPlugin.canceledTaskIds.contains(task.taskId))) {
                deleteTempFile()
                deletePublishedRangeFile()
                if (rangeTotal != null && BDPlugin.canceledTaskIds.contains(task.taskId)) result = TaskStatus.canceled
            } else if (result == TaskStatus.failed || (rangeTotal != null && result == TaskStatus.notFound)) {
                // A marked prefix survives a final failure or 404 for a user retry.
                prepResumeAfterFailure()
            }
            return result
        } finally {
            // Rejected headers must not leave an unread video response active.
            if (rangeTotal != null) {
                connection.disconnect()
                if (BDPlugin.canceledTaskIds.contains(task.taskId)) {
                    deleteTempFile()
                    deletePublishedRangeFile()
                }
            }
        }
    }

    /** Process the response to the GET or POST request on this [connection]
     *
     * Returns the [TaskStatus]
     */
    override suspend fun process(
        connection: HttpURLConnection,
    ): TaskStatus {
        // Check if the file should be skipped
        val skipThreshold = prefs.getInt(BDPlugin.keyConfigSkipExistingFiles, -1)
        if (rangeTotal == null && skipThreshold != -1) {
            val filePath = task.filePath(context.appContext)
            val file = File(filePath)
            if (file.exists()) {
                val fileSize = file.length()
                if (fileSize > skipThreshold * 1024L * 1024L) {
                    responseStatusCode = 304
                    return TaskStatus.complete
                }
            }
        }

        responseStatusCode = connection.responseCode
        val total = rangeTotal
        if (total != null) {
            Log.i(TAG, "Range response ${task.taskId}: ${connection.url}, status=$responseStatusCode, headers=${connection.headerFields}")
            if (UriUtils.containsUri(task.directory) || UriUtils.containsUri(task.filename)) {
                throw IOException("Range recovery requires a file destination: ${task.directory}/${task.filename}")
            }
            if (connection.responseCode in 200..206) validateRangeResponse(connection, total)
        }
        if (connection.responseCode in 200..206) {
            // determine if we are using Uri or not.  Uri means pause/resume not allowed
            val directoryUri = UriUtils.uriFromStringValue(task.directory)
            val usesUri = directoryUri != null
            eTagHeader = if (total == null) connection.headerFields["ETag"]?.first()
                else rangeHeader(connection, "ETag")
            val acceptRangesHeader = connection.headerFields["Accept-Ranges"]
            serverAcceptsRanges =
                acceptRangesHeader?.first() == "bytes" || connection.responseCode == 206
            if (task.allowPause && !usesUri) {
                taskCanResume = serverAcceptsRanges
                processCanResume(
                    task,
                    taskCanResume
                )
            }
            isResume = isResume && connection.responseCode == 206  // confirm resume response
            if (isResume && !prepareResume(connection)) {
                deleteTempFile()
                return TaskStatus.failed
            }
            if (isResume && (eTagHeader != eTag || eTag?.startsWith("W/") == true)) {
                deleteTempFile()
                Log.i(TAG, "Cannot resume: ETag is not identical, or is weak")
                taskException = TaskException(
                    ExceptionType.resume,
                    description = "Cannot resume: ETag is not identical, or is weak"
                )
                return TaskStatus.failed
            }
            // Determine destination - either [destFilePath] or [destUri]
            // If no filename is set, get from headers or url, and update the task
            var destFilePath = task.filePath(context.appContext)
            var (uriFilename, destUri) = unpack(task.filename)
            if (!task.hasFilename()) {
                // If no filename is set, get from headers or url, and update the task
                if (usesUri) {
                    uriFilename = suggestFilename(connection.headerFields, task.url)
                    if (uriFilename.isEmpty()) {
                        uriFilename = "${Random.nextInt().absoluteValue}"
                    }
                    task = task.copyWith(
                        filename = if (destUri == null) uriFilename else UriUtils.pack(
                            uriFilename,
                            destUri
                        )
                    )
                } else {
                    destFilePath = destFilePath(connection)
                }
            }
            extractResponseHeaders(connection.headerFields)
            extractContentType(connection.headerFields)
            val contentLength = if (total == null) getContentLength(connection.headerFields, task)
                else total - startByte
            // determine tempFile, or set to null if Uri is used
            val tempFile = if (!usesUri) {
                val applicationSupportPath =
                    baseDirPath(context.appContext, BaseDirectory.applicationSupport)
                val cachePath = baseDirPath(context.appContext, BaseDirectory.temporary)
                if (applicationSupportPath == null || cachePath == null) {
                    throw IllegalStateException("External storage is requested but not available")
                }
                val tempDir =
                    when (PreferenceManager.getDefaultSharedPreferences(context.appContext)
                        .getInt(BDPlugin.keyConfigUseCacheDir, -2)) {
                        0 -> File(cachePath) // 'always'
                        -1 -> File(applicationSupportPath) // 'never'
                        else -> {
                            // 'whenAble' -> determine based on cache quota
                            val storageManager =
                                context.appContext.getSystemService(Context.STORAGE_SERVICE) as StorageManager
                            val cacheQuota = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                                storageManager.getCacheQuotaBytes(
                                    storageManager.getUuidForPath(
                                        File(cachePath)
                                    )
                                )
                            } else {
                                50L shl (20)  // for older OS versions, assume 50MB
                            }
                            if (contentLength < cacheQuota / 2) File(cachePath) else File(
                                applicationSupportPath
                            )
                        }
                    }
                if (!tempDir.exists()) {
                    tempDir.mkdirs()
                }
                tempFilePath =
                    tempFilePath.ifEmpty { "${tempDir.absolutePath}/com.bbflight.background_downloader${Random.nextInt()}" }

                // confirm enough storage space for download
                if (insufficientSpace(context.appContext, contentLength)) {
                    Log.i(
                        TAG,
                        "Insufficient space to store the file to be downloaded for taskId ${task.taskId}"
                    )
                    taskException = TaskException(
                        ExceptionType.fileSystem,
                        description = "Insufficient space to store the file to be downloaded"
                    )
                    return TaskStatus.failed
                }
                File(tempFilePath)
            } else {
                null
            }
            val outputStream: java.io.OutputStream = if (tempFile != null) {
                FileOutputStream(tempFile, isResume)
            } else {
                // no tempFile, because we have a Uri
                uriFilename = uriFilename ?: "unknown"
                if (directoryUri!!.scheme == "file") {
                    // fileUri is converted to a File, then to a FileOutputStream
                    if (destUri == null) {
                        // need to create file at directory
                        val dirObject = directoryUri.toFile()
                        val destFile = File(dirObject, uriFilename)
                        destUri = Uri.fromFile(destFile)
                        // Store destination Uri in task
                        task = task.copyWith(filename = UriUtils.pack(uriFilename, destUri))
                        FileOutputStream(destFile) // return outputStream
                    } else {
                        // use destination Uri that was set in previous attempt
                        FileOutputStream(destUri.toFile())
                    }
                } else {
                    // other URL scheme will be attempted to resolve using content resolver
                    val resolver = context.appContext.contentResolver
                    // create destination Uri if not already exists
                    val documentFile = DocumentFile.fromTreeUri(context.appContext, directoryUri)
                    destUri = destUri ?: documentFile?.createFile(task.mimeType, uriFilename)?.uri
                    if (destUri == null) {
                        val message =
                            "Failed to create document within directory with URI: $directoryUri"
                        Log.e(TAG, message)
                        taskException = TaskException(
                            ExceptionType.fileSystem,
                            description = message
                        )
                        return TaskStatus.failed
                    }
                    val newFilename = getFilenameFromUri(destUri)
                    if (newFilename.isNotEmpty()) {
                        uriFilename = newFilename
                    }
                    task = task.copyWith(filename = UriUtils.pack(uriFilename, destUri))
                    val os = resolver.openOutputStream(destUri)
                    if (os == null) {
                        val message = "Failed to open output stream for URI: $destUri"
                        Log.e(TAG, message)
                        taskException = TaskException(
                            ExceptionType.fileSystem,
                            description = message
                        )
                        return TaskStatus.failed
                    } else os
                }
            }
            BDPlugin.remainingBytesToDownload[task.taskId] = contentLength
            determineRunInForeground(task, contentLength) // sets 'runInForeground'
            context.updateEstimatedNetworkBytes(contentLength, 0L)
            // transfer the bytes from the server to the output stream
            val transferBytesResult: TaskStatus
            if (total == null) {
                BufferedInputStream(connection.inputStream).use { inputStream ->
                    transferBytesResult = transferBytes(inputStream, outputStream, contentLength, task)
                }
                outputStream.flush()
                outputStream.close()
            } else {
                rangeBodyStarted = true
                // Reject excess bytes before writing them into a reusable prefix.
                val checkedOutput = object : FilterOutputStream(outputStream) {
                    override fun write(buffer: ByteArray, offset: Int, length: Int) {
                        if (length.toLong() > contentLength - bytesTotal) {
                            invalidRangeBody = true
                            throw IOException("Range body exceeds $contentLength bytes for ${task.url}")
                        }
                        out.write(buffer, offset, length)
                    }
                }
                checkedOutput.use { output ->
                    BufferedInputStream(connection.inputStream).use { input ->
                        transferBytesResult = transferBytes(input, output, contentLength, task)
                    }
                    output.flush()
                }
            }
            // act on the result of the bytes transfer
            when (transferBytesResult) {
                TaskStatus.complete -> {
                    if (total != null && tempFile?.length() != total) {
                        throw IOException("Incomplete range file: ${tempFile?.length()} of $total bytes for ${task.url}")
                    }
                    if (total != null && BDPlugin.canceledTaskIds.contains(task.taskId)) {
                        deleteTempFile()
                        return TaskStatus.canceled
                    }
                    if (tempFile == null) {
                        Log.i(
                            TAG, "Successfully downloaded taskId ${task.taskId} to URI $destUri"
                        )
                    } else {
                        // move file from its temp location to the destination
                        val destFile = File(destFilePath)
                        val dir = destFile.parentFile!!
                        if (!dir.exists()) {
                            dir.mkdirs()
                        }
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                            withContext(Dispatchers.IO) {
                                Files.move(
                                    tempFile.toPath(),
                                    destFile.toPath(),
                                    StandardCopyOption.REPLACE_EXISTING
                                )
                                if (total != null) publishedRangeFile = destFile
                            }
                            setFileOwnership(destFile)
                        } else {
                            tempFile.copyTo(destFile, overwrite = true)
                            if (total != null) publishedRangeFile = destFile
                            deleteTempFile()
                            setFileOwnership(destFile)
                        }
                        Log.i(
                            TAG, "Successfully downloaded taskId ${task.taskId} to $destFilePath"
                        )
                    }
                    return TaskStatus.complete
                }

                TaskStatus.canceled -> {
                    cleanup(usesUri, destUri)
                    Log.i(TAG, "Canceled taskId ${task.taskId}")
                    return TaskStatus.canceled
                }

                TaskStatus.paused -> {
                    BDPlugin.pausedTaskIds.remove(task.taskId)
                    if (taskCanResume) {
                        Log.i(TAG, "Task ${task.taskId} paused")
                        processResumeData(
                            ResumeData(
                                task, tempFilePath, bytesTotal + startByte, eTagHeader
                            ), prefs
                        )
                        return TaskStatus.paused
                    }
                    if (BDPlugin.tasksToReEnqueue.contains(task) && serverAcceptsRanges) {
                        // pause was triggered by re-enqueue request due to WiFi requirement change
                        // so we only store local resumeData without posting it
                        Log.i(TAG, "Task ${task.taskId} paused in order to re-enqueue")
                        BDPlugin.localResumeData[task.taskId] = ResumeData(
                            task, tempFilePath, bytesTotal + startByte, eTagHeader
                        )
                        return TaskStatus.paused
                    }
                    Log.i(TAG, "Task ${task.taskId} cannot resume, therefore pause failed")
                    taskException = TaskException(
                        ExceptionType.resume,
                        description = "Task was paused but cannot resume"
                    )
                    cleanup(usesUri, destUri)
                    return TaskStatus.failed
                }

                TaskStatus.enqueued -> {
                    // Special status, in this context means that the task timed out
                    // so if allowed, pause it and schedule the resume task immediately
                    if (!task.allowPause) {
                        Log.i(TAG, "Task ${task.taskId} timed out")
                        taskException =
                            TaskException(
                                ExceptionType.connection,
                                description = "Task timed out"
                            )
                        return TaskStatus.failed
                    }
                    if (taskCanResume) {
                        Log.i(
                            TAG,
                            "Task ${task.taskId} paused due to timeout, will resume in 1 second"
                        )
                        val start = bytesTotal + startByte
                        BDPlugin.doEnqueue(
                            context.appContext,
                            task,
                            notificationConfigJsonString,
                            ResumeData(task, tempFilePath, start, eTagHeader),
                            1000
                        )
                        return TaskStatus.paused
                    }
                    Log.i(TAG, "Task ${task.taskId} timed out and cannot pause/resume")
                    taskException =
                        TaskException(ExceptionType.connection, description = "Task timed out")
                    cleanup(usesUri, destUri)
                    return TaskStatus.failed
                }

                TaskStatus.failed -> {
                    if (total == null) prepResumeAfterFailure()
                    return TaskStatus.failed
                }

                else -> {
                    Log.e(TAG, "Unknown transferBytesResult $transferBytesResult")
                    cleanup(usesUri, destUri)
                    return TaskStatus.failed
                }
            }
        } else {
            // HTTP response code not OK
            Log.i(
                TAG,
                "Response code ${connection.responseCode} for taskId ${task.taskId}"
            )
            val errorContent = responseErrorContent(connection)
            taskException = TaskException(
                ExceptionType.httpResponse, httpResponseCode = connection.responseCode,
                description = if (errorContent?.isNotEmpty() == true) errorContent else connection.responseMessage
            )
            return if (connection.responseCode == 404) {
                responseBody = errorContent
                TaskStatus.notFound
            } else {
                TaskStatus.failed
            }
        }
    }

    /**
     * Return destination filePath where the filename is set based on response headers, and
     * update the [task] accordingly
     */
    private fun destFilePath(connection: HttpURLConnection): String {
        val destFilePath = task.filePath(context.appContext)
        task = task.withSuggestedFilenameFromResponseHeaders(
            context.appContext,
            connection.headerFields,
            unique = true
        )
        val dirName = File(destFilePath).parent ?: ""
        return if (dirName.isEmpty()) task.filename else "$dirName/${task.filename}"
    }


    /**
     * Return true if this is a resume, and resume is possible,
     * given [tempFilePath] and [requiredStartByte]
     * */
    override fun determineIfResume(): Boolean {
        // set tempFilePath from resume data, or "" if a new tempFile is needed
        requiredStartByte = context.getInputLong(keyStartByte, 0)
        if (requiredStartByte == 0L) {
            return false
        }
        eTag = context.getInputString(keyETag)
        tempFilePath = if (requiredStartByte > 0) context.getInputString(keyResumeDataData) ?: ""
        else ""
        val tempFile = File(tempFilePath)
        val total = rangeTotal
        if (total != null) {
            if (requiredStartByte in 1 until total && tempFile.isFile && tempFile.length() == requiredStartByte) {
                return true
            }
            Log.i(TAG, "Cannot resume range file $tempFilePath: length=${tempFile.length()}, offset=$requiredStartByte, total=$total")
            deleteTempFile()
            tempFilePath = ""
            requiredStartByte = 0
            return false
        }
        if (tempFile.exists()) {
            val tempFileLength = tempFile.length()
            if (tempFileLength == requiredStartByte) {
                return true
            } else {
                // attempt to truncate the file to the expected size
                Log.d(
                    TAG,
                    "File length = ${tempFile.length()} vs requiredStartByte = $requiredStartByte"
                )
                if (tempFileLength > requiredStartByte && Build.VERSION.SDK_INT >= 26) {
                    try {
                        val fileChannel =
                            FileChannel.open(tempFile.toPath(), StandardOpenOption.WRITE)
                        fileChannel.truncate(requiredStartByte)
                        fileChannel.close()
                        Log.d(TAG, "Truncated temp file to desired length")
                        return true
                    } catch (e: IOException) {
                        e.printStackTrace()
                    }
                }
                Log.i(TAG, "Partially downloaded file is corrupted, resume not possible")
            }
        } else {
            Log.i(TAG, "Partially downloaded file not available, resume not possible")
        }
        return false
    }

    /** Prepare for resume if possible
     *
     * Returns true if task can continue, false if task failed.
     * Extracts and parses Range headers, and truncates temp file
     */
    private fun prepareResume(connection: HttpURLConnection): Boolean {
        if (tempFilePath.isEmpty()) {
            return false
        }
        if (rangeTotal != null) {
            // The exact response was already checked before opening the file.
            if (File(tempFilePath).length() != requiredStartByte) return false
            startByte = requiredStartByte
            return true
        }
        val contentRanges = connection.headerFields["Content-Range"]
        if (contentRanges == null || contentRanges.size > 1) {
            Log.i(
                TAG, "Could not " +
                        "process partial response Content-Range"
            )
            return false
        }
        val range = contentRanges.first()
        val contentRangeRegEx = Regex("(\\d+)-(\\d+)/(\\d+)")
        val matchResult = contentRangeRegEx.find(range)
        if (matchResult == null) {
            Log.i(TAG, "Could not process partial response Content-Range $range")
            taskException = TaskException(
                ExceptionType.resume,
                description = "Could not process partial response Content-Range $range"
            )
            return false
        }
        val start = matchResult.groups[1]?.value?.toLong()!!
        val tempFile = File(tempFilePath)
        val tempFileLength = tempFile.length()
        startByte = start - taskRangeStartByte // relative to start of range
        if (startByte > tempFileLength) {
            Log.i(TAG, "Offered range not feasible: $range with startByte $startByte")
            taskException = TaskException(
                ExceptionType.resume,
                description = "Offered range not feasible: $range with startByte $startByte"
            )
            return false
        }
        // resume possible, set start conditions
        try {
            RandomAccessFile(tempFilePath, "rw").use { it.setLength(startByte) }
        } catch (_: IOException) {
            Log.i(TAG, "Could not truncate temp file")
            taskException =
                TaskException(
                    ExceptionType.resume,
                    description = "Could not truncate temp file"
                )
            return false
        }
        return true
    }

    /**
     * Attempt to allow resume after failure by sending resume data
     * back to Dart
     *
     * If this is not possible, the temp file will be deleted
     */
    private suspend fun prepResumeAfterFailure() {
        val total = rangeTotal
        if (total != null) {
            val file = File(tempFilePath)
            val length = file.length()
            val validPrefix = if (rangeBodyStarted) !invalidRangeBody && length == startByte + bytesTotal
                else isResume && length == requiredStartByte
            // Kept whatever the remaining retries after cuts and HTTP errors: Dart
            // refills the budget after progress and keeps the ResumeData after a
            // final failure. A final response contradicting the prefix drops it.
            val contradicted = rejectedRangeResponse && task.retriesRemaining <= 0
            if (!contradicted && file.isFile && length in 1 until total && validPrefix) {
                val retainedETag = if (rangeBodyStarted) eTagHeader else eTag
                Log.i(TAG, "Retaining range file ${task.taskId}: $tempFilePath, offset=$length, total=$total, ETag=$retainedETag")
                processResumeData(ResumeData(task, tempFilePath, length, retainedETag), prefs)
            } else {
                deleteTempFile()
            }
        } else if (serverAcceptsRanges && bytesTotal + startByte > 1 shl 20) {
            // if failure can be resumed, post resume data
            processResumeData(
                ResumeData(
                    task, tempFilePath, bytesTotal + startByte, eTagHeader
                ), prefs
            )
        } else {
            // if it cannot be resumed, delete the temp file
            deleteTempFile()
        }
    }

    /** A rejected response must never alter an already validated prefix. */
    private fun validateRangeResponse(connection: HttpURLConnection, total: Long) {
        val offset = if (isResume) requiredStartByte else 0L
        val expectedRange = "bytes $offset-${total - 1}/$total"
        val contentRange = rangeHeader(connection, "Content-Range")
        val encoding = rangeHeader(connection, "Content-Encoding")
        val length = rangeHeader(connection, "Content-Length")
        if (connection.responseCode != 206 || contentRange != expectedRange ||
            (encoding != null && !encoding.equals("identity", ignoreCase = true)) ||
            (length != null && length.toLongOrNull() != total - offset)) {
            rejectedRangeResponse = true
            throw IOException("Invalid range response for ${task.url}: status=${connection.responseCode}, Content-Range=$contentRange, Content-Encoding=$encoding, Content-Length=$length, expected=$expectedRange")
        }
    }

    private fun rangeHeader(connection: HttpURLConnection, name: String): String? {
        val values = connection.headerFields.entries
            .filter { it.key?.equals(name, ignoreCase = true) == true }
            .flatMap { it.value }
        if (values.isEmpty()) return null
        if (values.size != 1) throw IOException("Duplicate $name for ${task.url}: $values")
        return values.single().trim()
    }

    private fun deletePublishedRangeFile() {
        // Only delete a final file actually published by this runner.
        publishedRangeFile?.let { file ->
            if (!file.delete() && file.exists()) Log.w(TAG, "Could not delete canceled range file ${file.absolutePath}")
        }
    }

    /**
     * Deletes the temp file associated with this download
     */
    private fun deleteTempFile() {
        if (tempFilePath.isNotEmpty()) {
            try {
                val tempFile = File(tempFilePath)
                tempFile.delete()
            } catch (_: IOException) {
                Log.i(TAG, "Could not delete temp file at $tempFilePath")
            }
        }
    }

    /**
     * Deletes the destination Uri at [uri]
     */
    private fun deleteDestinationUri(uri: Uri) {
        try {
            context.appContext.contentResolver.delete(uri, null, null)
        } catch (_: Exception) {
            Log.i(TAG, "Could not delete file at $uri")
        }
    }

    /**
     * Cleanup by deleting the temp file or destination uri
     */
    private fun cleanup(usesUri: Boolean, destUri: Uri?) {
        if (usesUri && destUri != null) {
            deleteDestinationUri(destUri)
        } else {
            deleteTempFile()
        }
    }

    /**
     * Sets the group ownership of the downloaded file.
     *
     * Determines the app's GID and then calls Os.chown. Logs success or failure.
     * Likely to fail if the file is in external storage.
     *
     * Reason for changing ownership os to unmark file as a cache file (issue #498)
     */
    private fun setFileOwnership(destFile: File) {
        try {
            val finalPath = destFile.absolutePath
            val gid = context.appContext.applicationInfo.uid // App's own GID
            Os.chown(finalPath, -1, gid) // -1 for UID means keep current owner
        } catch (e: ErrnoException) {
            // Log the error, common if the filesystem doesn't support chown or due to permissions.
            // Check OsConstants for specific errno values if needed.
            Log.w(
                TAG,
                "Failed to change group ownership for ${destFile.absolutePath}: ${e.message}"
            )
        }
    }

}
