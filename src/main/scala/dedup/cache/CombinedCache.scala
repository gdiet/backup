package dedup.cache

import dedup.{ClassLogging, memChunk}

import java.nio.file.StandardOpenOption.{CREATE_NEW, READ, SPARSE, WRITE}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong

object CombinedCache extends ClassLogging {
  // see https://stackoverflow.com/questions/13713557/scala-accessing-protected-field-of-companion-objects-trait
  @inline override protected def trace_(msg: => String): Unit = super.trace_(msg)
  @inline override protected def debug_(msg: => String): Unit = super.debug_(msg)
  @inline override protected def info_ (msg: => String): Unit = super.info_ (msg)
  @inline override protected def warn_ (msg: => String): Unit = super.warn_ (msg)
  @inline override protected def error_(msg:    String): Unit = super.error_(msg)
  @inline override protected def error_(msg: String, e: Throwable): Unit = super.error_(msg, e)
}

/** Caches byte arrays with positions, where the byte arrays are not necessarily contiguous. Useful for representing
  * a singe file's contents in a virtual file system.
  *
  * Instances are thread safe. */
class CombinedCache(availableMem: AtomicLong, tempFilePath: Path, initialSize: Long) {
  import CombinedCache._

  private var _size: Long = initialSize
  def size: Long = synchronized(_size)

  private var _written: Boolean = false
  def written: Boolean = synchronized(_written)

  private val zeroCache = new Allocation
  private val memCache = new MemCache(availableMem)
  private var maybeChannelCache: Option[ChannelCache] = None

  private def channelCache = maybeChannelCache.getOrElse {
    println(s"Create cache file $tempFilePath") // FIXME make debug_
    val channel = Files.newByteChannel(tempFilePath, WRITE, CREATE_NEW, SPARSE, READ)
    new ChannelCache(channel).tap(c => maybeChannelCache = Some(c))
  }

  /** Truncates the cache to the provided size. Zero-pads if the cache size increases. */
  def truncate(size: Long): Unit = synchronized {
    if (size != _size) {
      if (size > _size) {
        zeroCache.allocate(_size, size - _size)
      } else {
        zeroCache.keep(size)
        memCache.keep(size)
        maybeChannelCache.foreach(_.keep(size))
      }
      _written = true
      _size = size
    }
  }

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = if (data.length > 0) synchronized {
    if (!memCache.write(position, data)) channelCache.write(position, data)
    if (position > _size) zeroCache.allocate(_size, position - _size)
    _written = true
    _size = math.max(_size, position + data.length)
  }

  /** Reads data from the cache. Throws an exception if the request exceeds the cache size.
    *
    * @return The remaining holes in the cached data. */
  def read[D: DataSink](position: Long, size: Long, sink: D): Vector[(Long, Long)] = synchronized {
    require(_size >= position + size, s"Requested $position $size exceeds ${_size}.")
    // Read from memory cache.
    memCache.read(position, size).flatMap {
      case Right(data) => LazyList(Right(data))
      // Fill holes from channel cache.
      case Left(position -> size) => maybeChannelCache.map(_.read(position, size)).getOrElse(LazyList(Left(position -> size)))
    }.flatMap {
      case Right(data) => LazyList(Right(data))
      case Left(position -> size) =>
        // Fill holes from zero allocations cache.
        zeroCache.read(position, size).flatMap {
          case Left(left) =>
            LazyList(Left(left))
          case Right(localPos -> localSize) =>
            LazyList.range(0L, localSize, memChunk)
              .map(off => Right(localPos + off -> new Array[Byte](math.min(memChunk, localSize - off).asInt)))
        }
    }.flatMap {
      case Right(pos -> data) => sink.write(pos - position, data); None
      case Left(gap) => Some(gap)
    }.toVector
  }
}
