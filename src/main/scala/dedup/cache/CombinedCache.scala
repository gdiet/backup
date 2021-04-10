package dedup.cache

import dedup.memChunk

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

/** Caches byte arrays with positions, where the byte arrays are not necessarily contiguous. Useful for representing
  * a singe file's contents in a virtual file system. */
class CombinedCache(availableMem: AtomicLong, tempFilePath: Path, initialSize: Long) extends AutoCloseable {

  private var _size: Long = initialSize
  def size: Long = _size

  private var _written: Boolean = false
  def written: Boolean = _written

  private val zeroCache = new Allocation
  private val memCache = new MemCache(availableMem)
  private var maybeChannelCache: Option[ChannelCache] = None

  private def channelCache = maybeChannelCache.getOrElse(
    new ChannelCache(tempFilePath).tap(c => maybeChannelCache = Some(c))
  )

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(size: Long): Unit = if (size != _size) {
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

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = if (data.length > 0) {
    // Clear the area in all caches.
    if (position < _size) {
      memCache.clear(position, data.length)
      maybeChannelCache.foreach(_.clear(position, data.length))
      zeroCache.clear(position, data.length)
    }
    // Write the area.
    if (!memCache.write(position, data)) channelCache.write(position, data)
    // Allocate zeros if writing starts beyond end of file.
    if (position > _size) zeroCache.allocate(_size, position - _size)
    _written = true
    _size = math.max(_size, position + data.length)
  }

  /** Reads data from the cache. Throws an exception if the request exceeds the cache size.
    *
    * @return The remaining holes in the cached data. */
  def read[D: DataSink](position: Long, size: Long, sink: D): Vector[(Long, Long)] = {
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

  override def close(): Unit = maybeChannelCache.foreach(_.close())
}
