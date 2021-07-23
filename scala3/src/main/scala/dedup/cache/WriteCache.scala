package dedup
package cache

import java.util.concurrent.atomic.AtomicLong
import java.nio.file.Path
import dedup.util.ClassLogging

/** Caches byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * Used keep track of changes to a singe file's contents for as long as that file is kept open.
  *
  * Not thread safe.
  *
  * @param temp Temp directory to use.
  * @param initialSize Size of the underlying file when the cache is instantiated. */
class WriteCache(availableMem: AtomicLong, temp: Path, initialSize: Long) extends AutoCloseable with ClassLogging:

  private var _size: Long = initialSize
  def          size: Long = _size

  private var _written: Boolean = false
  def          written: Boolean = _written

  private val memCache  = MemCache(availableMem)
  private val zeroCache = Allocation()
  private var fileCache = FileCache(temp)

  /** For debugging purposes. */
  override def toString: String =
    s"$getClass: mem $memCache / file $fileCache / zero $zeroCache"

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(newSize: Long): Unit = if newSize != size then guard(s"truncate $size -> $newSize") {
    require(newSize >= 0, s"newSize: $newSize")
    if newSize > size then
      zeroCache.allocate(position = size, size = newSize - size)
    else
      memCache .keep(newSize)
      zeroCache.keep(newSize)
      fileCache.keep(newSize)
    _written = true
    _size = newSize
  }

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = if data.length > 0 then guard(s"write $position [${data.length}]") {
    require(data.length <= memChunk, s"Data array too large: [${data.length}]")
    // Clear the area in all caches.
    if position < size then
      memCache .clear(position, data.length)
      zeroCache.clear(position, data.length)
      fileCache.clear(position, data.length)
    // Allocate zeros if writing starts beyond end of file.
    if position > size then zeroCache.allocate(size, position - size)
    // Write the area.
    if !memCache.write(position, data) then fileCache.write(position, data)
    _written = true
    _size = math.max(size, position + data.length)
  }

  /** Reads cached byte areas from this [[WriteCache]].
    *
    * @param position position to start reading at.
    * @param size     number of bytes to read.
    * @return A lazy list of (position, gapSize | byte array]).
    * @throws IllegalArgumentException if `offset` / `size` exceed the bounds of the cached area. */
  def read(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] = if size < 1 then LazyList() else
    require(position + size <= _size, s"Read $position/$size beyond end of cache $_size.")
    if size == 0 then LazyList() else guard(s"read($position, $size)") {
      val sizeToRead = math.min(size, _size - position) // TODO can probably be removed, see above require
      memCache.read(position, sizeToRead).flatMap {
        case right @ _ -> Right(_)      => LazyList(right)
        case position -> Left(holeSize) => fileCache.readData(position, holeSize) // Fill holes from channel cache.
      }.flatMap {
        case right @ _ -> Right(_)      => LazyList(right)
        case position -> Left(holeSize) => zeroCache.readData(position, holeSize) // Fill holes from zero cache.
      }
    }

  override def close(): Unit =
    memCache .close()
    fileCache.close()
