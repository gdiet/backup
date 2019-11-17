package dedup

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.SortedMap

/** This class is the storage interface for the dedup server. It keeps data from files currently open in volatile
 *  storage entries (memory or disk), writing them to long term storage when they are closed. The long term store
 *  that way is effectively a sequential write once / random access read many times storage.
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

  def read(id: Long, dataId: Long, startStop: StartStop)(offset: Long, requestedSize: Int): Array[Byte] = {
    assumeLogged(id > 0, s"id > 0 ... $id")
    assumeLogged(dataId > 0, s"dataId > 0 ... $dataId")
    assumeLogged(offset >= 0, s"offset >= 0 ... $offset")
    assumeLogged(requestedSize >= 0, s"requestedSize >= 0 ... $requestedSize")
    assumeLogged(requestedSize < 1000000, s"requestedSize < 1000000 ... $requestedSize")
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
      // FIXME test
      val readLength = if (start + length > longTermSize) (longTermSize - start).toInt else length
      (start, longTermStore.read(startStop.start + start, readLength) ++ new Array[Byte](length - readLength))
    }
    (chunksRead ++ chunks.map(e => e.position -> e.data)).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, longTermStoreSize: => Long): Long =
    entries.getEntry(id, dataId).map(_._1).getOrElse(longTermStoreSize)

  def truncate(id: Long, dataId: Long, startStop: StartStop, newSize: Long): Unit = {
    def zeros(offset: Long, size: Long): Seq[Entry] =
      if (size == 0) Seq()
      else if (size <= 524288) Seq(entries.newEntry(id, dataId, offset, new Array[Byte](size.toInt)))
      else zeros(offset + 524288, size - 524288) :+ entries.newEntry(id, dataId, offset, new Array[Byte](size.toInt))
    val longTermSize = startStop.size
    entries.getEntry(id, dataId) match {
      case None =>
        entries.setOrReplace(id, dataId, newSize, zeros(longTermSize, math.max(newSize - longTermSize, 0)))
      case Some(oldSize -> chunks) =>
        val newChunks = chunks.collect { case entry if entry.position < newSize =>
          val dropRight = math.max(0, entry.position + entry.length - newSize).toInt
          entry.drop(0, dropRight)
        }
        val zeroPadding = zeros(oldSize, math.max(newSize - oldSize, 0))
        entries.setOrReplace(id, dataId, newSize, newChunks ++ zeroPadding)
    }
  }

  def delete(id: Long, dataId: Long): Unit =
    entries.delete(id, dataId)

  def write(id: Long, dataId: Long, longTermSize: => Long)(offset: Long, data: Array[Byte]): Unit =
    // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    // TODO document resolution of this issue
    data.grouped(524288).foldLeft(0) { case (chunkOffset, bytes) =>
      internalWrite(id, dataId, longTermSize)(offset + chunkOffset, bytes)
      chunkOffset + 524288
    }

  private def internalWrite(id: Long, dataId: Long, longTermSize: => Long)(offset: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getEntry(id, dataId).getOrElse(longTermSize -> Seq())
    val newSize = math.max(fileSize, offset + data.length)
    val combinedChunks = chunks :+ (chunks.find(_.position == offset) match {
      case None => entries.newEntry(id, dataId, offset, data)
      case Some(previousData) =>
        if (data.length >= previousData.length) entries.newEntry(id, dataId, offset, data)
        else previousData.tap(_.write(data))
    })
    val (toMerge, others) = combinedChunks.partition { entry =>
      (entry.position < offset && entry.position + entry.length > offset) ||
        (entry.position >= offset && entry.position < offset + data.length)
    }
    val reduced = toMerge.sortBy(_.position).reduceLeft[Entry] { case (dataA, dataB) =>
      if (dataA.position + dataA.length >= dataB.position + dataB.length) dataA
      else dataA ++ dataB.drop((dataA.position + dataA.length - dataB.position).toInt, 0)
    }
    val merged = others :+ reduced
    entries.setOrReplace(id, dataId, newSize, merged)
  }

  def persist(id: Long, dataId: Long, startStop: StartStop)(writePosition: Long, dataSize: Long): Unit = {
    for { offset <- 0L until dataSize by 524288; chunkSize = math.min(524288, dataSize - offset).toInt } {
      val chunk = read(id, dataId, startStop)(offset, chunkSize)
      longTermStore.write(writePosition + offset, chunk)
    }
  }
}
