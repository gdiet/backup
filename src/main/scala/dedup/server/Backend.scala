package dedup
package server

object Backend:
  /** Milliseconds to delay write operations in order to keep cache size manageable. */
  def cacheLoadDelay: Long = bytesInPersistQueue.get() * persistQueueSize.get() / 1000000000
  private val persistQueueSize = java.util.concurrent.atomic.AtomicLong()
  private val bytesInPersistQueue = java.util.concurrent.atomic.AtomicLong()

  def writeAlgorithm(data: Iterator[(Long, Array[Byte])], toAreas: Seq[DataArea], write: (Long, Array[Byte]) => Unit): Unit =
    @annotation.tailrec
    def doStore(areas: Seq[DataArea], data: Array[Byte]): Seq[DataArea] =
      areas match
        case Seq() =>
          problem("write.algorithm.1", s"Remaining data areas are empty, data size is ${data.length}")
          Seq()
        case head +: rest =>
          if head.size == data.length then
            write(head.start, data)
            rest
          else if head.size > data.length then
            write(head.start, data)
            head.drop(data.length) +: rest
          else
            val intSize = head.size.toInt // always smaller than MaxInt, see above "if head.size > data.length"
            write(head.start, data.take(intSize))
            doStore(rest, data.drop(intSize))

    val remaining = data.foldLeft(toAreas) { case (storeAt, (_, bytes)) => doStore(storeAt, bytes) }
    ensure("write.algorithm.2", remaining.isEmpty, s"Remaining data areas not empty: $remaining")


final class Backend(settings: Settings) extends AutoCloseable with util.ClassLogging:
  private val db = dedup.db.Database(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val lts: store.LongTermStore = store.LongTermStore(settings.dataDir, settings.readonly)
  private val handles = Handles(settings.tempPath)
  private val freeAreas = server.FreeAreas(db.freeAreas())

  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext =
    concurrent.ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newSingleThreadExecutor())

  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: String): Option[TreeEntry] = db.entry(path)
  /** @return The path elements of this file system path. */
  def pathElements(path: String): Array[String] = db.pathElements(path)
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry] = db.entry(path)

  /** @return The child entries of the tree entry, an empty [[Seq]] if the parent entry does not exist. */
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)

  /** @return The child entry by name or None if the child entry is not found. */
  def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)

  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = db.mkDir(parentId, name)

  def setTime(id: Long, newTime: Long): Unit = db.setTime(id, newTime)

  def synchronizeTreeModification[T](f: => T): T = db.synchronizeTreeModification(f)
  
  def renameMove(id: Long, newParentId: Long, newName: String): Boolean = db.renameMove(id, newParentId, newName)

  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean =
    db.mkFile(newParentId, newName, file.time, file.dataId).isDefined

  /** Deletes a tree entry unless it has children.
    * @return [[false]] if the tree entry has children. */
  def deleteChildless(entry: TreeEntry): Boolean = db.deleteChildless(entry.id)

  /** @return The size of the file, 0 if the file entry does not exist. */
  def size(file: FileEntry): Long = handles.cachedSize(file.id).getOrElse(db.logicalSize(file.dataId))

  /** Create a virtual file handle so read/write operations can be done on the file.
    * For each [[open]] or [[createAndOpen]], a corresponding [[release]] call is required for normal operation. */
  def open(file: FileEntry): Unit = handles.open(file.id, file.dataId)

  /** Create file and a virtual file handle so read/write operations can be done on the file.
    * For each [[open]] or [[createAndOpen]], a corresponding [[release]] call is required for normal operation.
    * @return Some(fileId) or None if a child entry with the same name already exists. */
  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] =
    db.mkFile(parentId, name, time, DataId(-1)).tap(_.foreach { fileId =>
      if !handles.open(fileId, DataId(-1)) then log.warn(s"File handle (write) was already present for file $fileId.")
    })

  /** Releases a virtual file handle. Triggers a write-through if no other handles are open for the file.
    * For each [[open]] or [[createAndOpen]], a corresponding [[release]] call is required for normal operation.
    *
    * @return False if called without corresponding [[open]] or [[createAndOpen]]. */
  def release(fileId: Long): Boolean =
    handles.release(fileId) match
      case Failed => false
      case None => true
      case Some(entrySize) => enqueue(fileId, entrySize); true

  private def enqueue(fileId: Long, entrySize: Long): Unit =
    Backend.persistQueueSize.incrementAndGet()
    Backend.bytesInPersistQueue.addAndGet(entrySize)
    log.trace(s"Enqueue file $fileId size $entrySize - cache load ${Backend.cacheLoadDelay}")
    singleThreadStoreContext.execute(() => try {
      val handle = handles.get(fileId).getOrElse(failure(s"No handle to store for file $fileId."))
      val entry = handle.persisting.lastOption.getOrElse(failure(s"No entry to store for file $fileId."))
      log.debug(s"Persist file $fileId / data ID ${handle.dataId} / size ${entry.size}.")

      // For 0-length data entry explicitly set dataId -1 because it might have contained something else before.
      if entry.size == 0 then writeDataIdAndRemove(fileId, DataId(-1), entry) else

        val ltsParts = db.parts(handle.dataId)
        def data: Iterator[(Long, Array[Byte])] = entry.read(0, entry.size).flatMap {
          case position -> Right(data) => Iterator(position -> data)
          case position -> Left(offset) => readFromLts(ltsParts, position, offset)
        }
        // Calculate hash
        val md = java.security.MessageDigest.getInstance(hashAlgorithm)
        data.foreach(entry => md.update(entry._2))
        val hash = md.digest()
        // Check if already known
        db.dataEntry(hash, entry.size) match
          // Already known, simply link
          case Some(dataId) =>
            log.trace(s"Persisted $fileId - content known, linking to dataId $dataId")
            writeDataIdAndRemove(fileId, dataId, entry)
          // Not yet known, store ...
          case None =>
            // Reserve storage space
            val reserved = freeAreas.reserve(entry.size)
            // Write to storage
            Backend.writeAlgorithm(data, reserved, lts.write)
            // Save data entries
            val dataId = db.newDataId()
            reserved.zipWithIndex.foreach { case (dataArea, index) =>
              log.debug(s"Data ID $dataId size ${entry.size} - persisted at ${dataArea.start} size ${dataArea.size}")
              db.insertDataEntry(dataId, index + 1, entry.size, dataArea.start, dataArea.stop, hash)
            }
            log.trace(s"Persisted $fileId - new content, dataId $dataId")
            writeDataIdAndRemove(fileId, dataId, entry)

    } catch { case t: Throwable => log.error(s"Persisting file $fileId failed", t); throw t })

  private def writeDataIdAndRemove(fileId: Long, newDataId: DataId, entry: DataEntry): Unit =
    db.setDataId(fileId, newDataId)
    handles.removePersisted(fileId, newDataId)
    entry.close()
    Backend.persistQueueSize.decrementAndGet()
    Backend.bytesInPersistQueue.addAndGet(-entry.size)

  /** Truncates the cached file to a new size. Zero-pads if the file size increases.
    * @return `false` if called without createAndOpen or open. */
  def truncate(fileId: Long, newSize: Long): Boolean = handles.truncate(fileId, newSize)

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. Note that the byte arrays may be kept in memory, so make sure e.g.
    *             using defensive copy (Array.clone) that they are not modified later.
    * @return `false` if called without createAndOpen or open. */
  def write(fileId: Long, data: Iterator[(Long, Array[Byte])]): Boolean = handles.write(fileId, db.logicalSize, data)

  /** Provides the requested number of bytes from the referenced file
    * unless end-of-file is reached - in that case stops there.
    *
    * @param fileId        Id of the file to read from.
    * @param offset        Offset in the file to start reading at, must be >= 0.
    * @param requestedSize Number of bytes to read.
    * @return The (position -> bytes) read or [[None]] if the file is not open. */
  // Note that previous implementations provided atomic reads, but this is not really necessary...
  def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] =
    handles.get(fileId).map(_.readLock { case Handle(_, dataId, current, persisting) => // TODO move code to handles?
      val dataEntries = current ++: persisting
      lazy val fileSize = dataEntries.headOption.map(_.size).getOrElse(db.logicalSize(dataId))
      lazy val parts = db.parts(dataId)
      readFromDataEntries(offset, math.min(requestedSize, fileSize - offset), dataEntries)
        .flatMap {
          case position -> Left(size) => readFromLts(parts, position, size)
          case position -> Right(data) => Iterator(position -> data)
        }
    })

  /** @return All available data for the area specified from the provided queue of [[DataEntry]] objects. */
  private def readFromDataEntries(position: Long, holeSize: Long, remaining: Seq[DataEntry]): Iterator[(Long, Either[Long, Array[Byte]])] =
    remaining match
      case Seq() => Iterator(position -> Left(holeSize))
      case head +: tail =>
        head.read(position, holeSize).flatMap {
          case (innerPosition, Left(innerHoleSize)) => readFromDataEntries(innerPosition, innerHoleSize, tail)
          case other => Iterator(other)
        }

  /** From the long term store, reads file content defined by `parts`.
    *
    * @param parts    List of (offset, size) defining the file content parts to read.
    *                 `readFrom` + `readSize` should not exceed summed part sizes unless
    *                 `parts` is the empty list that is used for blacklisted entries.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    *         If `parts` is too small, the data is filled up with zeros.
    * @throws IllegalArgumentException if `readFrom` is negative or `readSize` is less than 1.
    */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    log.trace(s"readFromLts(readFrom: $readFrom, readSize: $readSize, parts: $parts)")
    ensure("read.lts.offset", readFrom >= 0, s"Read offset $readFrom must be >= 0.")
    ensure("read.lts.size", readSize >= 0, s"Read size $readSize must be > 0.")
    val partsToReadFrom = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part@(partPosition, partSize)) =>
        val distance = readFrom - currentOffset
        if distance > partSize then currentOffset + partSize -> result
        else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
        else currentOffset + partSize -> (result :+ part)
    }._2

    def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
      log.trace(s"readFromLts recurse(remainingParts: $remainingParts, readSize: $readSize, resultOffset: $resultOffset)")
      remainingParts match
        case Seq() =>
          if parts.nonEmpty then log.warn(s"Could not fully read $readSize bytes starting at $readFrom from these parts: $parts")
          LazyList.range(resultOffset, readSize, memChunk.toLong).map(
            offset => offset -> new Array[Byte](math.min(memChunk, readSize - offset).toInt)
          )
        case (partPosition, partSize) +: rest =>
          if partSize < readSize then lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
          else lts.read(partPosition, readSize, resultOffset)

    recurse(partsToReadFrom, readSize, readFrom).iterator

  /** Clean up and release resources. */
  override def close(): Unit =
    handles.shutdown().foreach { case (fileId, entrySize) => enqueue(fileId, entrySize) }
    singleThreadStoreContext.shutdown()
    singleThreadStoreContext.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS)
    db.close()
    log.info("Shutdown complete.")