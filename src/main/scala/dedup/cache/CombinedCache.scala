package dedup.cache

import java.util.concurrent.atomic.AtomicLong

class CombinedCache(availableMem: AtomicLong, initialSize: Long) {
  private var _size: Long = initialSize
  private var _written: Boolean = false

  private val zeroCache = new Allocation
  private val memCache = new MemCache(availableMem)

  /** Truncates the cache to the provided size. Zero-pads if the cache size increases. */
  def truncate(size: Long): Unit = synchronized { if (size != _size) {
    if (size > _size) {
      zeroCache.allocate(_size, size - _size)
    } else {
      zeroCache.keep(size)
      memCache.keep(size)
    }
    _written = true
    _size = size
  } }

}
