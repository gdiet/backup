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

  extension(m: Array[Byte])
    override protected def length: Long = m.length
    override protected def dropped: Unit = availableMem.addAndGet(length)
    override protected def drop(distance: Long): Array[Byte] =
      require(distance < length && distance > 0, s"Distance: $distance")
      availableMem.addAndGet(distance)
      m.drop(distance.asInt)
    override protected def keep(distance: Long): Array[Byte] =
      require(distance < length && distance > 0, s"Distance: $distance")
      ???
    override protected def split(distance: Long): (Array[Byte], Array[Byte]) =
      require(distance < length && distance > 0, s"Distance: $distance")
      ???

  @annotation.tailrec
  private def tryAcquire(size: Long): Boolean =
    val avail = availableMem.get()
    if avail < size then false
    else if availableMem.compareAndSet(avail, avail - size) then true
    else tryAcquire(size)

  override def keep(newSize: Long): Unit =
    // In addition, deallocate all higher entries.
    val higher = entries.tailMap(newSize)
    super.keep(newSize)
    higher.forEach((_, data) => availableMem.addAndGet(data.length))

  /** Assumes that the area to write is clear.
    *
    * @return `false` if data is not cached because not enough free memory is available. */
  def write(position: Long, data: Array[Byte]): Boolean =
    tryAcquire(data.length).tap(if (_) entries.put(position, data))

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] =
    areasInSection(position, size)

  override def close(): Unit = ???
