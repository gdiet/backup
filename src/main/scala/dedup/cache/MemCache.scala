package dedup
package cache

import dedup.util.ClassLogging

import java.lang.Runtime.getRuntime as rt
import java.util.concurrent.atomic.AtomicLong

object MemCache extends ClassLogging:
  // Give room for database and memory management peculiarities, see startupCheck.
  private val cacheLimit: Long = math.max(0, (rt.maxMemory - 64000000) * 7 / 10)
  log.info(s"Memory cache size: ${readableBytes(cacheLimit)}")
  val availableMem: AtomicLong = AtomicLong(cacheLimit)

  /** Allocate 90% of the free heap with byte arrays sized [[dedup.memChunk]], then free the heap again.
    * This is to ensure [[cache.MemCache]] will work as expected.
    *
    * @see https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    * @see https://stackoverflow.com/questions/68331703/java-big-byte-arrays-use-more-heap-than-expected */
  def startupCheck(): Unit =
    val freeMemory = rt.maxMemory - rt.totalMemory + rt.freeMemory
    val numberOfChunks = (freeMemory / 10 * 9 / memChunk).asInt
    log.debug(s"Checking memory cache with ${readableBytes(numberOfChunks.toLong * memChunk)}.")
    Vector.fill(numberOfChunks)(new Array[Byte](memChunk))
    log.debug(s"Memory cache check ok.")

/** Caches in memory byte arrays with positions, where the byte arrays are not necessarily contiguous. */
class MemCache(availableMem: AtomicLong) extends CacheBase[Array[Byte]] with AutoCloseable:
  override protected def length(m: Array[Byte]): Long = m.length
  override protected def merge (m: Array[Byte], n: Array[Byte]): Option[Array[Byte]]        = if m.length + n.length > memChunk then None else Some(m ++ n)
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
    tryAcquire(data.length).tap(if _ then {
      clear(position, data.length)
      entries.put(position, data)
      mergeIfPossible(position)
    })

  override def close(): Unit =
    import scala.jdk.CollectionConverters.MapHasAsScala
    release(entries.asScala.map(_._2.length.toLong))
