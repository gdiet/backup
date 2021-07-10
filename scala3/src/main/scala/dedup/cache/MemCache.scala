package dedup
package cache

import dedup.util.ClassLogging
import java.util.concurrent.atomic.AtomicLong
import Runtime.{getRuntime => rt}

trait AvailableMem:
  def free(size: Long): Unit
  def tryAcquire(size: Long): Boolean

class ConstReservation(available: AtomicLong) extends AvailableMem: // FIXME test class, not for production ?
  def free(size: Long): Unit = available.addAndGet(size)
  @annotation.tailrec
  final def tryAcquire(size: Long): Boolean =
    val avail = available.get()
    if avail < size then false
    else if available.compareAndSet(avail, avail - size) then true
    else tryAcquire(size)

class MemManager extends AvailableMem:
  private var lastChecked = now
  def free(size: Long): Unit = MemCache.availableMem.addAndGet(size)
  def tryAcquire(size: Long): Boolean =
    if lastChecked + 100000 < now then
      lastChecked = now
      MemCache.availableMem.set(MemCache.limit)
    acquire(size)
  @annotation.tailrec
  private def acquire(size: Long): Boolean =
    val avail = MemCache.availableMem.get()
    if avail < size then false
    else if MemCache.availableMem.compareAndSet(avail, avail - size) then true
    else acquire(size)

object MemCache extends ClassLogging:
  def limit = (rt.maxMemory()/10*9 - rt.totalMemory() + rt.freeMemory()).tap(size =>
    log.info(s"Memory cache size: ${readableBytes(size)}")
  )
  val availableMem = AtomicLong(limit)

/** Caches in memory byte arrays with positions, where the byte arrays are not necessarily contiguous. */
class MemCache(availableMem: AvailableMem) extends CacheBase[Array[Byte]] with AutoCloseable:
  override protected def length(m: Array[Byte]): Long = m.length
  override protected def merge (m: Array[Byte], n: Array[Byte]): Option[Array[Byte]]        = if m.length + n.length > memChunk then None else Some(m ++ n)
  override protected def drop  (m: Array[Byte], distance: Long): Array[Byte]                = m.drop(distance.asInt)
  override protected def keep  (m: Array[Byte], distance: Long): Array[Byte]                = m.take(distance.asInt)
  override protected def split (m: Array[Byte], distance: Long): (Array[Byte], Array[Byte]) = m.splitAt(distance.asInt)

  override protected def release(sizes: => Iterable[Long]): Unit = availableMem.free(sizes.sum)

  /** @return `false` if data is not cached because not enough free memory is available. */
  def write(position: Long, data: Array[Byte]): Boolean =
    availableMem.tryAcquire(data.length).tap(if _ then
      clear(position, data.length)
      entries.put(position, data)
      mergeIfPossible(position)
    )

  override def close(): Unit =
    import scala.jdk.CollectionConverters.MapHasAsScala
    release(entries.asScala.map(_._2.length.toLong))
