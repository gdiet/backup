package dedup2

import java.util.concurrent.atomic.AtomicLong

case class MemCached(override val start: Long, private val data: Array[Byte]) extends Cached {
  override def end: Long = start + data.length
  override def read(from: Long, to: Long): Array[Byte] = {
    require(from >= start, s"$from >= $start")
    require(to <= end, s"$to <= $end")
    data.slice((from - start).toInt, (to-start).toInt)
  }
  override def take(size: Long): MemCached = {
    require(size < data.length, s"$size < ${data.length}")
    copy(data = data.take(size.toInt))
  }
  override def drop(size: Long): MemCached = {
    require(size < data.length, s"$size < ${data.length}")
    copy(data = data.drop(size.toInt))
  }
  override def toString: String = s"MemCached($start, size ${data.length}, data ${data.take(10).toSeq}...)"
}

trait Cached {
  def start: Long
  def end: Long
  def read(from: Long, to: Long): Array[Byte]
  def take(size: Long): Cached
  def drop(size: Long): Cached
}
object Cached {
  val cacheLimit: Long = math.max(16000000, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val cacheUsed = new AtomicLong(0)
}

object DataEntry extends ClassLogging {
  // see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  @inline override protected def trace_(msg: => String): Unit = super.trace_(msg)
  @inline override protected def debug_(msg: => String): Unit = super.debug_(msg)
  @inline override protected def info_ (msg: => String): Unit = super.info_ (msg)
  @inline override protected def warn_ (msg: => String): Unit = super.warn_ (msg)
  @inline override protected def error_(msg:    String): Unit = super.error_(msg)
  @inline override protected def error_(msg: String, e: Throwable): Unit = super.error_(msg, e)
}

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long) { import DataEntry._
  trace_(s"Create with base data ID $baseDataId.")
  /** position -> data */
  private var cached: Seq[Cached] = Seq()
  private var _written: Boolean = false
  private var _size: Long = initialSize

  def written: Boolean = synchronized(_written)
  def size: Long = synchronized(_size)

  /** readUnderlying is (dataId, offset, size) => data */
  def read(position: Long, size: Int, readUnderlying: (Long, Long, Int) => Data): Data = synchronized {
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
    withUnderLyingEnd :+ new Array((endOfRead - nextPos).toInt)
  }

  def truncate(size: Long): Unit = synchronized {
    cached = cached.collect {
      case entry if entry.end <= size => entry
      case entry if entry.start <= size => entry.take(size - entry.start)
    }
    _written = true
    _size = size
  }

  def write(start: Long, dataToWrite: Array[Byte]): Unit = synchronized {
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
