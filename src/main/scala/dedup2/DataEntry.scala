package dedup2

import org.slf4j.LoggerFactory

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long = 0) {
  private val log = LoggerFactory.getLogger("dedup.DataEntry")

  log.info(s"create $baseDataId -> $this")
  /** position -> data */
  private var mem: Map[Long, Array[Byte]] = Map()
  private var _written: Boolean = false
  private var _size: Long = initialSize

  def written: Boolean = synchronized(_written)
  def size: Long = synchronized(_size)

  /** readUnderlying is (dataId, offset, size) => data */
  def read(position: Long, size: Int, readUnderlying: (Long, Long, Int) => Array[Byte]): Array[Byte] = synchronized {
    log.info(s"$baseDataId -> $this")
    log.info("" + mem.view.mapValues(_.length).toMap)
    val endOfRead = math.min(position + size, this._size) // FIXME test
    val candidates =
      mem.view.filterKeys(p => p >= position && p < endOfRead)
        .toSeq.sortBy(_._1)
    val (currentPos, result) = candidates.foldLeft[(Long, Array[Byte])](position -> Array()) { case ((currentPos, result), (pos, data)) =>
      val head = if (currentPos < pos) readUnderlying(baseDataId, currentPos, (pos - currentPos).toInt) else Array()
      val tail = data.take(math.min(data.length, endOfRead - currentPos).toInt)
      pos + tail.length -> (result ++ head ++ tail)
    }
    if (currentPos == endOfRead) result else result ++ readUnderlying(baseDataId, currentPos, (endOfRead-currentPos).toInt)
  }
  def truncate(size: Long): Unit = synchronized { // FIXME test
    mem = mem.collect { case entry @ position -> data if position < size =>
      if (position + data.length > size) position -> data.take((size - position).toInt) else entry
    }
    _written = true
    _size = size
  }
  def write(offset: Long, dataToWrite: Array[Byte]): Unit = synchronized { // FIXME test
    val endOfWrite = offset + dataToWrite.length
    mem = mem.flatMap { case position -> data =>
      val prefix =
        if (position >= offset) None
        else Some(position -> data.take(math.min(offset - position, data.length).toInt))
      val postOff = position + data.length - endOfWrite
      val postfix =
        if (postOff <= 0) None
        else Some(position -> data.take(math.min(postOff, data.length).toInt))
      Seq(prefix, postfix).flatten
    } + (offset -> dataToWrite)
    _written = true
    _size = math.max(_size, endOfWrite)
  }
}
