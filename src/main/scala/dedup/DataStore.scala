package dedup

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.SortedMap

/** This is the storage interface for the dedup server. It keeps data from files currently open in volatile storage
 *  entries (memory or disk), writing them to long term storage when they are closed. That way the long term store
 *  is effectively a sequential write once / random access read many times storage.
 *
 *  This class is not thread safe.
 */
class DataStore(dataDir: String, tempPath: String, readOnly: Boolean) extends AutoCloseable {
  implicit private val log: Logger = LoggerFactory.getLogger(getClass)
  private val longTermStore = new LongTermStore(dataDir, readOnly)
  private val entries = new CacheEntries(tempPath, readOnly)
  import entries.Entry

  def writeProtectCompleteFiles(startPosition: Long, endPosition: Long): Unit =
    longTermStore.writeProtectCompleteFiles(startPosition, endPosition)

  override def close(): Unit = { entries.close(); longTermStore.close() }

  def ifDataWritten(id: Long, dataId: Long)(f: => Unit): Unit =
    if (entries.getEntry(id, dataId).nonEmpty) f

  // FIXME new
  def read(id: Long, dataId: Long, parts: Parts)(offset: Long, requestedSize: Int): Array[Byte] = try {
    assumeLogged(id > 0, s"id > 0 ... $id")
    assumeLogged(dataId > 0, s"dataId > 0 ... $dataId")
    assumeLogged(offset >= 0, s"offset >= 0 ... $offset")
    assumeLogged(requestedSize >= 0, s"requestedSize >= 0 ... $requestedSize")
    val longTermSize = parts.size
    val (fileSize, localEntries) = entries.getEntry(id, dataId).getOrElse(longTermSize -> Seq[Entry]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - offset), 0).toInt
    val cachedChunks: Seq[Entry] = localEntries.collect {
      case entry
        if (entry.position <  offset && entry.position + entry.length > offset) ||
          (entry.position >= offset && entry.position                < offset + sizeToRead) =>
        val dropLeft  = math.max(0, offset - entry.position).toInt
        val dropRight = math.max(0, entry.position + entry.length - (offset + sizeToRead)).toInt
        entry.drop(dropLeft, dropRight)
    }
    val partsNotCached = cachedChunks.foldLeft(SortedMap(offset -> sizeToRead)) { case (result, entry) =>
      val (readPosition, sizeToRead) = result.filter(_._1 <= entry.position).last
      result - readPosition ++ Seq(
        readPosition -> (entry.position - readPosition).toInt,
        entry.position + entry.length -> (readPosition + sizeToRead - entry.position - entry.length).toInt
      ).filterNot(_._2 == 0)
    }
    val chunksRead: SortedMap[Long, Array[Byte]] = partsNotCached.map { case (start, length) =>
      val readLength = math.min(math.max(longTermSize - start, 0), length).toInt
      val read = parts.range(start, readLength)
        .map{ case (start, stop) => longTermStore.read(start, (stop-start).toInt) }
        .foldLeft(Array[Byte]())(_ ++ _)
      (start, read ++ new Array[Byte](length - readLength))
    }
    (chunksRead ++ cachedChunks.map(e => e.position -> e.data)).values.reduce(_ ++ _)
  } catch {
    case e: Throwable =>
      log.error(s"DS: read($id, $dataId, $parts)($offset, $requestedSize)")
      log.error(s"DS: entries = $entries")
      throw e
  }

  // FIXME old
  def read(id: Long, dataId: Long, startStop: StartStop)(offset: Long, requestedSize: Int): Array[Byte] = try {
    assumeLogged(id > 0, s"id > 0 ... $id")
    assumeLogged(dataId > 0, s"dataId > 0 ... $dataId")
    assumeLogged(offset >= 0, s"offset >= 0 ... $offset")
    assumeLogged(requestedSize >= 0, s"requestedSize >= 0 ... $requestedSize")
    val longTermSize = startStop.size
    val (fileSize, localEntries) = entries.getEntry(id, dataId).getOrElse(longTermSize -> Seq[Entry]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - offset), 0).toInt
    val chunks: Seq[Entry] = localEntries.collect {
      case entry
        if (entry.position <  offset && entry.position + entry.length > offset) ||
           (entry.position >= offset && entry.position                < offset + sizeToRead) =>
        val dropLeft  = math.max(0, offset - entry.position).toInt
        val dropRight = math.max(0, entry.position + entry.length - (offset + sizeToRead)).toInt
        entry.drop(dropLeft, dropRight)
    }
    val chunksNotCached = chunks.foldLeft(SortedMap(offset -> sizeToRead)) { case (chunksToRead, entry) =>
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= entry.position).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (entry.position - readPosition).toInt,
        entry.position + entry.length -> (readPosition + sizeToRead - entry.position - entry.length).toInt
      ).filterNot(_._2 == 0)
    }
    val chunksRead: SortedMap[Long, Array[Byte]] = chunksNotCached.map { case (start, length) =>
      val readLength = math.min(math.max(longTermSize - start, 0), length).toInt
      (start, longTermStore.read(startStop.start + start, readLength) ++ new Array[Byte](length - readLength))
    }
    (chunksRead ++ chunks.map(e => e.position -> e.data)).values.reduce(_ ++ _)
  } catch {
    case e: Throwable =>
      log.error(s"DS: read($id, $dataId, $startStop)($offset, $requestedSize)")
      log.error(s"DS: entries = $entries")
      throw e
  }

  def size(id: Long, dataId: Long, longTermStoreSize: => Long): Long =
    entries.getEntry(id, dataId).map(_._1).getOrElse(longTermStoreSize)

  def truncate(id: Long, dataId: Long, startStop: StartStop, newSize: Long): Unit = {
    @annotation.tailrec
    def zeros(offset: Long, size: Long, acc: Seq[Entry], additionalMemory: Long = 0): Seq[Entry] =
      if (size == 0) acc
      else if (size <= memChunk) acc :+ entries.newEntry(id, dataId, offset, new Array[Byte](size.toInt), additionalMemory)
      else zeros(offset + memChunk, size - memChunk,
        acc :+ entries.newEntry(id, dataId, offset, new Array[Byte](memChunk), additionalMemory),
        additionalMemory + memChunk)
    val longTermSize = startStop.size
    entries.getEntry(id, dataId) match {
      case None =>
        entries.setOrReplace(id, dataId, newSize, zeros(longTermSize, math.max(newSize - longTermSize, 0), Seq()))
      case Some(oldSize -> chunks) =>
        val newChunks = chunks.collect { case entry if entry.position < newSize =>
          val dropRight = math.max(0, entry.position + entry.length - newSize).toInt
          entry.drop(0, dropRight)
        }
        val zeroPadding = zeros(oldSize, math.max(newSize - oldSize, 0), Seq())
        entries.setOrReplace(id, dataId, newSize, newChunks ++ zeroPadding)
    }
  }

  def delete(id: Long, dataId: Long): Unit =
    entries.delete(id, dataId)

  def write(id: Long, dataId: Long, longTermSize: => Long)(offset: Long, data: Array[Byte]): Unit =
    // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    // TODO document resolution of this issue
    data.grouped(memChunk).foldLeft(0) { case (chunkOffset, bytes) =>
      internalWrite(id, dataId, longTermSize)(offset + chunkOffset, bytes)
      chunkOffset + memChunk
    }

  private def internalWrite(id: Long, dataId: Long, longTermSize: => Long)(offset: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getEntry(id, dataId).getOrElse(longTermSize -> Seq())
    val newSize = math.max(fileSize, offset + data.length)

    var integrated = false
    val updated = chunks.map { entry =>
      if (entry.position <= offset && entry.position + entry.length >= offset + data.length) {
        integrated = true
        entry.tap(_.write((offset - entry.position).toInt, data))
      } else if (entry.position < offset && entry.position + entry.length > offset)
        entry.drop(0, (entry.position + entry.length - offset).toInt)
      else if (entry.position >= offset && entry.position < offset + data.length) {
        entry.drop(math.min(entry.length, (offset + data.length - entry.position).toInt), 0)
      } else entry
    }.filterNot(_.length == 0)
    if (integrated) entries.setOrReplace(id, dataId, newSize, updated)
    else entries.setOrReplace(id, dataId, newSize, updated :+ entries.newEntry(id, dataId, offset, data))
  }

  // FIXME new
  def persist(id: Long, dataId: Long, parts: Parts)(writePosition: Long, dataSize: Long): Unit = {
    for { offset <- 0L until dataSize by memChunk; chunkSize = math.min(memChunk, dataSize - offset).toInt } {
      val chunk = read(id, dataId, parts)(offset, chunkSize)
      longTermStore.write(writePosition + offset, chunk)
    }
  }

  // FIXME old
  def persist(id: Long, dataId: Long, startStop: StartStop)(writePosition: Long, dataSize: Long): Unit = {
    for { offset <- 0L until dataSize by memChunk; chunkSize = math.min(memChunk, dataSize - offset).toInt } {
      val chunk = read(id, dataId, startStop)(offset, chunkSize)
      longTermStore.write(writePosition + offset, chunk)
    }
  }
}
