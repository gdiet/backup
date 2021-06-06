package dedup
package cache

import java.util.concurrent.atomic.AtomicLong
import java.nio.file.Path
import dedup.util.ClassLogging

/** Caches byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * Used keep track of changes to a singe file's contents for as long as that file is kept open.
  *
  * @param temp Temp directory to use.
  * @param initialSize Size of the underlying file when the cache is instantiated. */
class FileCache(availableMem: AtomicLong, temp: Path, initialSize: Long) extends AutoCloseable with ClassLogging:

  private var _size: Long = initialSize
  def          size: Long = _size

  private var _written: Boolean = false
  def          written: Boolean = _written

  private val zeroCache = Allocation()
  private val memCache = MemCache(availableMem)
  private var maybeChannelCache: Option[ChannelCache] = None
  private def channelCache = maybeChannelCache.getOrElse(ChannelCache(temp).tap(c => maybeChannelCache = Some(c)))

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(newSize: Long): Unit = if newSize != _size then
    require(newSize >= 0, s"newSize: $newSize")
    if newSize > _size then
      zeroCache.allocate(_size, newSize - _size)
    else
      zeroCache.keep(newSize)
      memCache.keep(newSize)
      maybeChannelCache.foreach(_.keep(newSize))
    _written = true
    _size = newSize

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = ???

  /** @return LazyList((holePosition, holeSize) | (dataPosition, bytes)) where positions start at `position`. */
  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] = ???

  override def close(): Unit =
    maybeChannelCache.foreach(_.close())
