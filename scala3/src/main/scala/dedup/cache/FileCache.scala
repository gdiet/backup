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
  def size: Long = _size

  private var _written: Boolean = false
  def written: Boolean = _written

  private val zeroCache = Allocation()
  private val memCache = MemCache(availableMem)
  private var maybeChannelCache: Option[ChannelCache] = None
  private def channelCache = maybeChannelCache.getOrElse(ChannelCache(temp).tap(c => maybeChannelCache = Some(c)))

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(newSize: Long): Unit = if newSize != _size then
    if newSize > _size then
      zeroCache.allocate(_size, newSize - _size)
    else
      zeroCache.keep(newSize)
      memCache.keep(newSize)
      maybeChannelCache.foreach(_.keep(newSize))
    _written = true
    _size = newSize

  override def close(): Unit = ???

  // /** @return LazyList((holePosition, holeSize) | (dataPosition, bytes)) where positions start at `position`. */
  // def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] =
  //   ???
    // if size == 0 then LazyList() else
    //   log.trace(s"read(position = $position, size: $size)")
    //   // Read from memory cache.
    //   memCache.read(position, size).flatMap {
    //     case Right(data) => LazyList(Right(data))
    //     // Fill holes from channel cache.
    //     case Left(position -> size) => maybeChannelCache.map(_.read(position, size)).getOrElse(LazyList(Left(position -> size)))
    //   }.flatMap {
    //     case Right(data) => LazyList(Right(data))
    //     case Left(position -> size) =>
    //       // Fill holes from zero allocations cache.
    //       zeroCache.read(position, size).flatMap {
    //         case Left(left) =>
    //           LazyList(Left(left))
    //         case Right(localPos -> localSize) =>
    //           LazyList.range(0L, localSize, memChunk)
    //             .map(off => Right(localPos + off -> new Array[Byte](math.min(memChunk, localSize - off).asInt)))
    //       }
    //   }

end FileCache
