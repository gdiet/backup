package dedup
package server

object Backend:
  private val entryCount = new java.util.concurrent.atomic.AtomicLong()
  private val entriesSize = new java.util.concurrent.atomic.AtomicLong()
  def cacheLoad: Long = entriesSize.get() * entryCount.get()

class Backend(settings: Settings) extends AutoCloseable with util.ClassLogging:
  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val handles = Handles(settings.tempPath)

  override def close(): Unit =
    handles.shutdown() // FIXME handle the result
    lts.close()

  /** @return The size of the cached entry if any or the logical size of the file's data entry. */
  def size(fileEntry: FileEntry): Long =
    handles.cachedSize(fileEntry.id).getOrElse(db.logicalSize(fileEntry.dataId))

  /** @param fileId  Id of the file to truncate.
    * @param newSize The new size of the file, can be more, less or the same as before.
    * @return [[false]] if the file is not open. */
  def truncate(fileId: Long, newSize: Long): Boolean =
    handles.dataEntry(fileId, db.logicalSize).map(_.truncate(newSize)).isDefined

  /** @param fileId Id of the file to write to.
    * @param data   Iterator(position -> bytes).
    * @return [[false]] if the file is not open. */
  def write(fileId: Long, data: Iterator[(Long, Array[Byte])]): Boolean =
    handles.dataEntry(fileId, db.logicalSize).map(_.write(data)).isDefined

  /** Provides the requested number of bytes from the referenced file
    * unless end-of-file is reached - in that case stops there.
    *
    * @param fileId        Id of the file to read from.
    * @param offset        Offset in the file to start reading at, must be >= 0.
    * @param requestedSize Number of bytes to read.
    * @return A contiguous Iterator(position, bytes) or [[None]] if the file is not open. */
  // Note that previous implementations provided atomic reads, but this is not really necessary...
  def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] =
    handles.read(fileId, offset, requestedSize).map { case dataId -> read =>
      lazy val fileSize -> parts = db.logicalSize(dataId) -> db.parts(dataId)
      read.flatMap {
        case (position, Left(holeSize)) => readFromLts(parts, position, math.min(holeSize, fileSize - offset))
        case (position, Right(bytes)) => Iterator(position -> bytes)
      }
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
    def partsToReadFrom = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part@(partPosition, partSize)) =>
        val distance = readFrom - currentOffset
        if distance > partSize then currentOffset + partSize -> result
        else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
        else currentOffset + partSize -> (result :+ part)
    }._2
    if parts.nonEmpty then readRecurse(partsToReadFrom, readSize, readFrom).iterator
    else fillZeros(readFrom, readSize).iterator

  private def readRecurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
    remainingParts match
      case Seq() =>
        log.error(s"Could not fully read $readSize bytes.")
        fillZeros(resultOffset, readSize)
      case (partPosition, partSize) +: rest =>
        if partSize < readSize then lts.read(partPosition, partSize, resultOffset) #::: readRecurse(rest, readSize - partSize, resultOffset + partSize)
        else lts.read(partPosition, readSize, resultOffset)

  private def fillZeros(resultOffset: Long, readSize: Long): LazyList[(Long, Array[Byte])] =
    LazyList.range(resultOffset, readSize, memChunk.toLong).map(
      offset => offset -> new Array[Byte](math.min(memChunk, readSize - offset).toInt)
    )
