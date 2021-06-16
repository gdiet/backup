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
class MemCache(availableMem: AtomicLong) extends CacheBase[Array[Byte]] with AutoCloseable:
  override protected def length(m: Array[Byte]): Long = m.length
  override protected def drop  (m: Array[Byte], distance: Long): Array[Byte]                = m.drop(distance.asInt)
  override protected def keep  (m: Array[Byte], distance: Long): Array[Byte]                = m.take(distance.asInt)
  override protected def split (m: Array[Byte], distance: Long): (Array[Byte], Array[Byte]) = m.splitAt(distance.asInt)

  override protected def release(sizes: => Iterable[Long]): Unit = availableMem.addAndGet(sizes.sum)

  @annotation.tailrec
  private def tryAcquire(size: Long): Boolean =
    val avail = availableMem.get()
    if avail < size then false
    else if availableMem.compareAndSet(avail, avail - size) then true
    else tryAcquire(size)

  /** @return `false` if data is not cached because not enough free memory is available. */
  def write(position: Long, data: Array[Byte]): Boolean =
    tryAcquire(data.length).tap(if _ then
      clear(position, data.length)
      entries.put(position, data) // TODO try to merge entries
    )

  override def close(): Unit = ???
