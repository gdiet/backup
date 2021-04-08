package dedup.cache

import java.util.concurrent.atomic.AtomicLong

/** Caches in memory byte arrays with positions, where the byte arrays are not necessarily contiguous.
  *
  * Instances are not thread safe. */
class MemCache(available: AtomicLong) extends CacheBase[Array[Byte]] {
  override implicit protected val m: MemArea[Array[Byte]] = new ByteArrayArea(available)
// FIXME
//  @tailrec
//  private def tryAquire(size: Long): Boolean = {
//    val avail = availableMem.get()
//    if (avail < size) false
//    else if (availableMem.compareAndSet(avail, avail - size)) true
//    else tryAquire(size)
//  }

  /** Truncates the cached data to the provided size. */
  override def keep(newSize: Long): Unit = {
    // Remove higher entries (by keeping all strictly lower entries).
    entries = entries.headMap(newSize, false)
    // If necessary, trim highest entry.
    Option(entries.lastEntry()).foreach { case Entry(storedPosition, stored) =>
      val distance = newSize - storedPosition
      if (distance < stored.length) {
        entries.put(storedPosition, stored.take(distance.asInt))
//        release(stored.length - distance) // FIXME
      }
    }
  }

  /** Assumes that the area to write is clear.
    *
    * @return `false` if data is not cached because not enough free memory is available. */
  def write(position: Long, data: Array[Byte]): Boolean = { // FIXME if (tryAquire(data.length)) {
    // Store new entry.
    entries.put(position, data)
    true
  } // FIXME else false

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] =
    areasInSection(position, size)
}
