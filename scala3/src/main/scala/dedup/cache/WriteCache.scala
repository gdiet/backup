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

  /** Truncates the cache to a new size. Zero-pads if the cache size increases. */
  def truncate(newSize: Long): Unit = if newSize != _size then guard(s"truncate $_size -> $newSize") {
    require(newSize >= 0, s"newSize: $newSize")
    if newSize > _size then
      zeroCache.allocate(position = _size, size = newSize - _size)
    else
      memCache .keep(newSize)
      zeroCache.keep(newSize)
      fileCache.keep(newSize)
    _written = true
    _size = newSize
  }

  /** Writes data to the cache. Data size should not exceed `memChunk`. */
  def write(position: Long, data: Array[Byte]): Unit = if data.length > 0 then guard(s"write $position [${data.length}]") {
    require(data.length < memChunk, s"Data array too large: [${data.length}]")
    // Clear the area in all caches.
    if position < _size then
      memCache .clear(position, data.length)
      zeroCache.clear(position, data.length)
      fileCache.clear(position, data.length)
    // Allocate zeros if writing starts beyond end of file.
    if position > _size then zeroCache.allocate(_size, position - _size)
    // Write the area.
    if !memCache.write(position, data) then fileCache.write(position, data)
    _written = true
    _size = math.max(_size, position + data.length)
  }

  /** Reads cached byte areas from this [[WriteCache]].
    *
    * @param position position to start reading at.
    * @param size     number of bytes to read.
    * @return A lazy list of (position, gapSize | byte array]). */
  def read(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] = if size == 0 then LazyList() else guard(s"read($position, $size)") {
    memCache.read(position, size).flatMap {
      case right @ _ -> Right(_)  => LazyList(right)
      case position -> Left(size) => fileCache.readData(position, size) // Fill holes from channel cache.
    }.flatMap {
      case right @ _ -> Right(_)  => LazyList(right)
      case position -> Left(size) => zeroCache.readData(position, size) // Fill holes from zero cache.
    }
  }

  override def close(): Unit =
    memCache .close()
    fileCache.close()
