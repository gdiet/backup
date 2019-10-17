package dedup

import org.slf4j.LoggerFactory

import scala.collection.immutable.SortedMap

class DataStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val log = LoggerFactory.getLogger(getClass)
  val longTermStore = new LongTermStore(dataDir, readOnly)
  override def close(): Unit = longTermStore.close()

  sealed trait Entry {
    def write(data: Array[Byte]): Unit
    def length: Int
    def drop(left: Int, right: Int): Entry
    def data: Array[Byte]
    def ++(other: Entry): Entry
  }
  object Entry {
    def apply(id: Long, dataId: Long, position: Long, data: Array[Byte]): Entry =
      MemoryEntry(id, dataId, position, data)
  }
  case class MemoryEntry(id: Long, dataId: Long, position: Long, data: Array[Byte]) extends Entry {
    override def length: Int = data.length
    override def drop(left: Int, right: Int): Entry = copy(data = data.drop(left).dropRight(right))
    override def write(data: Array[Byte]): Unit = System.arraycopy(data, 0, this.data, 0, data.length)
    override def ++(other: Entry): Entry = Entry(id, dataId, position, data ++ other.data)
  }
  case class FileEntry(id: Long, dataId: Long, position: Long, length: Int) extends Entry {
    override def drop(left: Int, right: Int): Entry = copy(position = position + left, length = length - left - right)
    override def data: Array[Byte] = ???
    override def write(data: Array[Byte]): Unit = ???
    override def ++(other: Entry): Entry = ???
  }
  // Map(id/dataId -> size, Map(position, data))
  private var entries = Map[(Long, Long), (Long, Map[Long, Entry])]()
  private var memoryUsage = 0L

  def hasTemporaryData(id: Long, dataId: Long): Boolean = entries.contains(id -> dataId)

  def read(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, requestedSize: Int): Array[Byte] = {
    val (fileSize, localChunks) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Map[Long, Entry]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - position), 0).toInt
    val chunks: Map[Long, Entry] = localChunks.collect {
      case (chunkPosition, entry)
        if (chunkPosition <  position && chunkPosition + entry.length > position) ||
           (chunkPosition >= position && chunkPosition                < position + sizeToRead) =>
        val dropLeft  = math.max(0, position - chunkPosition).toInt
        val dropRight = math.max(0, chunkPosition + entry.length - (position + sizeToRead)).toInt
        chunkPosition + dropLeft -> entry.drop(dropLeft, dropRight)
    }
    val chunksToRead = chunks.foldLeft(SortedMap(position -> sizeToRead)) { case (chunksToRead, (chunkPosition, chunkData)) =>
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= chunkPosition).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (chunkPosition - readPosition).toInt,
        chunkPosition + chunkData.length -> (readPosition + sizeToRead - chunkPosition - chunkData.length).toInt
      ).filterNot(_._2 == 0)
    }
    val chunksRead: SortedMap[Long, Array[Byte]] =
      chunksToRead.map { case (start, length) => start -> longTermStore.read(ltStart + start, length) }
    (chunksRead ++ chunks.view.mapValues(_.data)).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Long =
    entries.get(id -> dataId).map(_._1).getOrElse(ltStop - ltStart)

  def delete(id: Long, dataId: Long, writeLog: Boolean = true): Unit = {
    memoryUsage = memoryUsage - entries.get(id -> dataId).toSeq.flatMap(_._2).map(_._2.length.toLong).sum
    if (writeLog) log.debug(s"Delete - memory usage $memoryUsage.")
    entries -= (id -> dataId)
  }

  def truncate(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Unit = {
    delete(id, dataId, writeLog = false)
    log.debug(s"Truncate - memory usage $memoryUsage.")
    entries += (id -> dataId) -> (0L -> Map())
  }

  def write(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Map[Long, Entry]())
    val newSize = math.max(fileSize, position + data.length)
    val combinedChunks = chunks + (chunks.get(position) match {
      case None => position -> Entry(id, dataId, position, data)
      case Some(previousData) =>
        if (data.length >= previousData.length) position -> Entry(id, dataId, position, data)
        else position -> previousData.tap(_.write(data))
    })
    val (toMerge, others) = combinedChunks.partition { case (chunkPosition, chunkData) =>
      (chunkPosition < position && chunkPosition + chunkData.length > position) ||
        (chunkPosition >= position && chunkPosition < position + data.length)
    }
    val reduced = toMerge.toSeq.sortBy(_._1).reduceLeft[(Long, Entry)] { case (posA -> dataA, posB -> dataB) =>
      if (posA + dataA.length >= posB + dataB.length) posA -> dataA
      else posA -> (dataA ++ dataB.drop((posA + dataA.length - posB).toInt, 0))
    }
    val merged = others + reduced
    delete(id, dataId, writeLog = false)
    memoryUsage += merged.values.map(_.length.toLong).sum
    log.debug(s"Write - memory usage $memoryUsage.")
    entries += (id -> dataId) -> (newSize -> merged)
  }
}
