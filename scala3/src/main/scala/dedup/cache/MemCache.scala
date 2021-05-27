package dedup
package cache

import dedup.util.ClassLogging
import java.util.concurrent.atomic.AtomicLong

object MemCache extends ClassLogging:
  private val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  log.info(s"Memory cache size: ${readableBytes(cacheLimit)}")
  val availableMem = new AtomicLong(cacheLimit)
end MemCache

/** Caches in memory byte arrays with positions, where the byte arrays are not necessarily contiguous. */
class MemCache(availableMem: AtomicLong) extends CacheBase[Array[Byte]] {
  override implicit protected val m: MemArea[Array[Byte]] = ??? // new ByteArrayArea(availableMem)

  @annotation.tailrec
  private def tryAcquire(size: Long): Boolean =
    val avail = availableMem.get()
    if avail < size then false
    else if availableMem.compareAndSet(avail, avail - size) then true
    else tryAcquire(size)

  /** Truncates the cached data to the provided size. */
  override def keep(newSize: Long): Unit =
    ???
    // // Remove higher entries (by keeping all strictly lower entries).
    // entries = entries.headMap(newSize, false)
    // // If necessary, trim highest entry.
    // Option(entries.lastEntry()).foreach { case Entry(storedPosition, stored) =>
    //   val distance = newSize - storedPosition
    //   if (distance < stored.length) entries.put(storedPosition, stored.take(distance.asInt))
    // }

  /** Assumes that the area to write is clear.
    *
    * @return `false` if data is not cached because not enough free memory is available. */
  def write(position: Long, data: Array[Byte]): Boolean =
    tryAcquire(data.length).tap(if (_) entries.put(position, data))

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] =
    areasInSection(position, size)
}
