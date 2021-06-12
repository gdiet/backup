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
class WriteCache(availableMem: AtomicLong, temp: Path, initialSize: Long) extends AutoCloseable with ClassLogging:

  private var _size: Long = initialSize
  def          size: Long = _size

  private var _written: Boolean = false
  def          written: Boolean = _written

  private val memCache = MemCache(availableMem)
  private val zeroCache = Allocation()
  private var maybeChannelCache: Option[ChannelCache] = None
  private def channelCache = maybeChannelCache.getOrElse(ChannelCache(temp).tap(c => maybeChannelCache = Some(c)))

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(newSize: Long): Unit = if newSize != _size then guard(s"truncate $_size -> $newSize") {
    require(newSize >= 0, s"newSize: $newSize")
    if newSize > _size then
      zeroCache.allocate(position = _size, size = newSize - _size)
    else
      memCache.keep(newSize)
      zeroCache.keep(newSize)
      maybeChannelCache.foreach(_.keep(newSize))
    _written = true
    _size = newSize
  }

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = if data.length > 0 then guard(s"write $position [${data.length}]") {
    // Clear the area in all caches.
    if position < _size then
      memCache.clear(position, data.length)
      zeroCache.clear(position, data.length)
      maybeChannelCache.foreach(_.clear(position, data.length))
    // Allocate zeros if writing starts beyond end of file.
    if position > _size then zeroCache.allocate(_size, position - _size)
    // Write the area.
    if !memCache.write(position, data) then channelCache.write(position, data)
    _written = true
    _size = math.max(_size, position + data.length)
  }

  /** @return LazyList((holePosition, holeSize) | (dataPosition, bytes)) where positions start at `position`. */
  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] = if size == 0 then LazyList() else guard(s"read($position, $size)") {
    // FIXME change return type to LazyList[Long, Either[(Long, Array[Byte])]]
    ???
//    // Read from memory cache.
//    memCache.read(position, size).flatMap {
//      case Right(data) => LazyList(Right(data))
//      // Fill holes from channel cache.
//      case left @ Left(position -> size) => ??? // maybeChannelCache.map(_.read(position, size)).getOrElse(LazyList(left))
//    }.flatMap {
//      case Right(data) => LazyList(Right(data))
//      case Left(position -> size) =>
//        // Fill holes from zero allocations cache.
//        zeroCache.read(position, size).flatMap {
//          case Right(localPos -> localSize) =>
//            LazyList.range(0L, localSize, memChunk.toLong)
//              .map(off => Right(localPos + off -> new Array[Byte](math.min(memChunk, localSize - off).asInt)))
//          case Left(data) => LazyList(Left(data))
//        }
//    }
  }

  override def close(): Unit =
    memCache.close()
    maybeChannelCache.foreach(_.close())
