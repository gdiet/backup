package dedup

import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong

object DataEntry extends ClassLogging {
  // see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  @inline override protected def trace_(msg: => String): Unit = super.trace_(msg)
  @inline override protected def debug_(msg: => String): Unit = super.debug_(msg)
  @inline override protected def info_ (msg: => String): Unit = super.info_ (msg)
  @inline override protected def warn_ (msg: => String): Unit = super.warn_ (msg)
  @inline override protected def error_(msg:    String): Unit = super.error_(msg)
  @inline override protected def error_(msg: String, e: Throwable): Unit = super.error_(msg, e)
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get
}

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable { import DataEntry._
  private val id = currentId.incrementAndGet()
  trace_(s"Create $id with base data ID $baseDataId.")
  /** position -> data */
  private var cached: Seq[Cached] = Seq()
  private def cacheSize: Long = cached.map(_.cacheSize).sum
  private var _written: Boolean = false
  private var _size: Long = initialSize

  private var maybeChannel: Option[SeekableByteChannel] = None
  private val path = tempDir.resolve(s"$id")
  private def channel = synchronized {
    maybeChannel.getOrElse {
      debug_(s"Create cache file $path")
      Files.newByteChannel(path, WRITE, CREATE_NEW, SPARSE, READ).tap(c => maybeChannel = Some(c))
    }
  }

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
    val oldCacheSize = cacheSize
    cached = cached.collect {
      case entry if entry.end <= size => entry
      case entry if entry.start <= size => entry.take(size - entry.start)
    }
    Cached.cacheUsed.addAndGet(cacheSize - oldCacheSize)
    _written = true
    _size = size
  }

  def write(start: Long, dataToWrite: Array[Byte]): Unit = synchronized {
    val oldCacheSize = cacheSize
    val end = start + dataToWrite.length
    cached = cached.flatMap { entry =>
      if (entry.start >= end || entry.end <= start) Some(entry)
      else if (entry.start >= start && entry.end <= end) None
      else if (entry.start < start) Some(entry.take(start - entry.start))
      else Some(entry.drop(end - entry.start))
    } :+ Cached(start, dataToWrite, channel)
    Cached.cacheUsed.addAndGet(cacheSize - oldCacheSize)
    _written = true
    _size = math.max(_size, end)
  }

  override def close(): Unit = synchronized {
    maybeChannel.foreach { channel =>
      channel.close()
      Files.delete(path)
      debug_(s"Closed & deleted cache file $path")
    }
    trace_(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
    Cached.cacheUsed.addAndGet(-cacheSize)
  }
}
