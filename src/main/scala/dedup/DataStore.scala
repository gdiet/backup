package dedup

import java.io.File

import scala.collection.immutable.SortedMap

class DataStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val longTermStore = new LongTermStore(dataDir, readOnly)
  private val entries = new CacheEntries(new File(dataDir, "../data-temp").getAbsoluteFile)
  import entries.Entry

  def writeProtectCompleteFiles(startPosition: Long, endPosition: Long): Unit =
    longTermStore.writeProtectCompleteFiles(startPosition, endPosition)

  override def close(): Unit = { entries.close(); longTermStore.close() }

  def ifDataWritten(id: Long, dataId: Long)(f: => Unit): Unit =
    if (entries.getEntry(id, dataId).nonEmpty) f

  def read(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, requestedSize: Int): Array[Byte] = {
    val (fileSize, localEntries) = entries.getEntry(id, dataId).getOrElse(ltStop - ltStart -> Seq[Entry]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - position), 0).toInt
    val chunks: Seq[Entry] = localEntries.collect {
      case entry
        if (entry.position <  position && entry.position + entry.length > position) ||
           (entry.position >= position && entry.position                < position + sizeToRead) =>
        val dropLeft  = math.max(0, position - entry.position).toInt
        val dropRight = math.max(0, entry.position + entry.length - (position + sizeToRead)).toInt
        entry.drop(dropLeft, dropRight)
    }
    val chunksToRead = chunks.foldLeft(SortedMap(position -> sizeToRead)) { case (chunksToRead, entry) =>
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= entry.position).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (entry.position - readPosition).toInt,
        entry.position + entry.length -> (readPosition + sizeToRead - entry.position - entry.length).toInt
      ).filterNot(_._2 == 0)
    }
    val chunksRead: SortedMap[Long, Array[Byte]] =
      chunksToRead.map { case (start, length) => start -> longTermStore.read(ltStart + start, length) }
    (chunksRead ++ chunks.map(e => e.position -> e.data)).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Long =
    entries.getEntry(id, dataId).map(_._1).getOrElse(ltStop - ltStart)

  def truncate(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Unit =
    entries.setOrReplace(id, dataId, 0, Seq())

  def delete(id: Long, dataId: Long): Unit =
    entries.delete(id, dataId)

  def write(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, data: Array[Byte]): Unit =
    // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    data.grouped(524288).foldLeft(0) { case (offset, bytes) =>
      internalWrite(id, dataId, ltStart, ltStop)(position + offset, bytes)
      offset + 524288
    }

  private def internalWrite(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getEntry(id, dataId).getOrElse(ltStop - ltStart -> Seq())
    val newSize = math.max(fileSize, position + data.length)
    val combinedChunks = chunks :+ (chunks.find(_.position == position) match {
      case None => entries.newEntry(id, dataId, position, data)
      case Some(previousData) =>
        if (data.length >= previousData.length) entries.newEntry(id, dataId, position, data)
        else previousData.tap(_.write(data))
    })
    val (toMerge, others) = combinedChunks.partition { entry =>
      (entry.position < position && entry.position + entry.length > position) ||
        (entry.position >= position && entry.position < position + data.length)
    }
    val reduced = toMerge.sortBy(_.position).reduceLeft[Entry] { case (dataA, dataB) =>
      if (dataA.position + dataA.length >= dataB.position + dataB.length) dataA
      else dataA ++ dataB.drop((dataA.position + dataA.length - dataB.position).toInt, 0)
    }
    val merged = others :+ reduced
    entries.setOrReplace(id, dataId, newSize, merged)
  }

  def persist(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(writePosition: Long, dataSize: Long): Unit = {
    for { offset <- 0L until dataSize by 524288; chunkSize = math.min(524288, dataSize - offset).toInt } {
      val chunk = read(id, dataId, ltStart, ltStop)(offset, chunkSize)
      longTermStore.write(writePosition + offset, chunk)
    }
  }
}
