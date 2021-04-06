package dedup.cache

import dedup.ClassLogging
import dedup.cache.CombinedCache._

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
  private var _size: Long = initialSize
  private var _written: Boolean = false

  private val zeroCache = new Allocation
  private val memCache = new MemCache(availableMem)
  private var maybeChannelCache: Option[ChannelCache] = None

  private def channelCache = maybeChannelCache.getOrElse {
    debug_(s"Create cache file $tempFilePath")
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

  def write(position: Long, data: Array[Byte]): Unit = if (data.length > 0) synchronized {
    if (!memCache.write(position, data)) channelCache.write(position, data)
    _written = true
    _size = math.max(_size, position + data.length)
  }
}
