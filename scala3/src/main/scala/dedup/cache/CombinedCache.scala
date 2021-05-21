package dedup.cache

import java.util.concurrent.atomic.AtomicLong
import java.nio.file.Path
import dedup.util.ClassLogging

/** Caches byte arrays with positions, where the byte arrays are not necessarily contiguous. Useful for representing
  * a singe file's contents in a virtual file system. */
class CombinedCache(availableMem: AtomicLong, tempFilePath: Path, initialSize: Long)  extends AutoCloseable with ClassLogging:

  private var _size: Long = initialSize
  def size: Long = _size

  private var _written: Boolean = false
  def written: Boolean = _written

  override def close(): Unit = ???

end CombinedCache
