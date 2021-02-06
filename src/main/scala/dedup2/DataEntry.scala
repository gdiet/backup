package dedup2

import org.slf4j.LoggerFactory

case class MemCached(start: Long, data: Array[Byte]) {
  def end: Long = start + data.length
  def read(from: Long, to: Long): Array[Byte] = {
    require(from >= start, s"$from >= $start")
    require(to <= end, s"$to <= $end")
    data.slice((from - start).toInt, (to-start).toInt)
  }
  def take(size: Long): MemCached = {
    require(size < data.length, s"$size < ${data.length}")
    copy(data = data.take(size.toInt))
  }
  def drop(size: Long): MemCached = {
    require(size < data.length, s"$size < ${data.length}")
    copy(data = data.drop(size.toInt))
  }
  override def toString: String = s"MemCached($start, size ${data.length}, data ${data.take(10).toSeq}...)"
}

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long = 0) {
  private val log = LoggerFactory.getLogger("dedup2.DataEn") // FIXME make static if at all necessary

  log.info(s"create $baseDataId -> $this")
  /** position -> data */
  private var cached: Seq[MemCached] = Seq()
  private var _written: Boolean = false
  private var _size: Long = initialSize

  def written: Boolean = synchronized(_written)
  def size: Long = synchronized(_size)

  /** readUnderlying is (dataId, offset, size) => data */
  def read(position: Long, size: Int, readUnderlying: (Long, Long, Int) => Data): Data = synchronized { // FIXME test
    log.info(s"read $baseDataId, size ${this.size} -> $this; cached: $cached")
    val sizeToReturn = math.max(0, math.min(this.size - position, size).toInt)
    val endOfRead = position + sizeToReturn
    val candidates = cached.filter(entry => entry.start < endOfRead && entry.end > position).sortBy(_.start)
    val (currentPos, result) = candidates.foldLeft[(Long, Data)](position -> Vector()) {
      case ((readPosition, result), cached) =>
        val startInCache = math.max(readPosition, cached.start)
        val head = if (readPosition == startInCache) Vector() else
          readUnderlying(baseDataId, readPosition, (cached.start - readPosition).toInt)
        val tail = cached.read(startInCache, math.min(endOfRead, cached.end))
        startInCache + tail.length -> (result ++ head :+ tail)
    }
    val nextPos -> withUnderLyingEnd =
      if (currentPos == endOfRead) currentPos -> result else {
        val end = readUnderlying(baseDataId, currentPos, (endOfRead-currentPos).toInt)
        val endLength = end.map(_.length).sum
        (currentPos + endLength) -> (result ++ end)
      }
    val x = withUnderLyingEnd :+ new Array((endOfRead - nextPos).toInt)
    x.tap(_ => log.info(s"read returns: " + x.reduce(_++_).toList))
  }

  def truncate(size: Long): Unit = synchronized { // FIXME test
    cached = cached.collect {
      case entry if entry.end <= size => entry
      case entry if entry.start <= size => entry.take(size - entry.start)
    }
    _written = true
    _size = size
  }

  def write(start: Long, dataToWrite: Array[Byte]): Unit = synchronized { // FIXME test
    val end = start + dataToWrite.length
    cached = cached.flatMap { entry =>
      if (entry.start >= end || entry.end <= start) Some(entry)
      else if (entry.start >= start && entry.end <= end) None
      else if (entry.start < start) Some(entry.take(start - entry.start))
      else Some(entry.drop(end - entry.start))
    } :+ MemCached(start, dataToWrite)
    _written = true
    _size = math.max(_size, end)
  }
}
