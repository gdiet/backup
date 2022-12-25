package dedup
package server

import java.util.concurrent.atomic.AtomicLong

final class Backend(settings: Settings) extends util.ClassLogging:
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val lts: store.LongTermStore = store.LongTermStore(settings.dataDir, settings.readonly)
  private val handles = Handles(settings.tempPath)

  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: String): Option[TreeEntry] = entry(pathElements(path))
  /** @return The path elements of this file system path. */
  def pathElements(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  /** @return The child entries of the tree entry, an empty [[Seq]] if the parent entry does not exist. */
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)

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
      case Handles.NotOpen => false
      case maybeEntry =>
        // FIXME handle Some() case
        true

  def child(parentId: Long, name: String): Option[TreeEntry] = ???
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = ???
  def setTime(id: Long, newTime: Long): Unit = ???
  def update(id: Long, newParentId: Long, newName: String): Boolean = ???
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = ???
  /** Deletes a tree entry unless it has children.
    * @return [[false]] if the tree entry has children. */
  def deleteChildless(entry: TreeEntry): Boolean = ???

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
    * @param receiver      The receiver of the bytes read.
    * @return The number of bytes read or [[None]] if the file is not open. */
  // Note that previous implementations provided atomic reads, but this is not really necessary...
  def read(fileId: Long, offset: Long, requestedSize: Long)(receiver: Iterator[(Long, Array[Byte])] => Any): Option[Long] =
    handles.get(fileId).map(_.readLock { case Handle(_, dataId, current, persisting) =>
      val dataEntries = current ++: persisting
      lazy val fileSize = dataEntries.headOption.map(_.size).getOrElse(db.logicalSize(dataId))
      lazy val parts = db.parts(dataId)
      val data = readFromDataEntries(offset, math.min(requestedSize, fileSize - offset), dataEntries)
        .flatMap {
          case position -> Left(size) => readFromLts(parts, position, size)
          case position -> Right(data) => Iterator(position -> data)
        }
      var sizeRead: Long = 0L
      receiver(data.tapEach(sizeRead += _._2.length))
      sizeRead
    })

  /** @return All available data for the area specified from the provided queue of [[DataEntry2]] objects. */
  private def readFromDataEntries(position: Long, holeSize: Long, remaining: Seq[DataEntry2]): Iterator[(Long, Either[Long, Array[Byte]])] =
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
  def shutdown(): Unit =
    handles.shutdown() // FIXME handle return value
    log.warn("SHUTDOWN NOT COMPLETELY IMPLEMENTED") // FIXME Backend.shutdown code missing
    db.close()
    log.info("Shutdown complete.")
