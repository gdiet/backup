package dedup.cache

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

trait Source {
  /** @param offset Position in source of the first byte to copy.
    * @param target Byte array to write bytes into.
    * @param index Position in target of the first byte to copy.
    * @param length Number of bytes to copy. */
  def get(offset: Long, target: Array[Byte], index: Int, length: Int): Unit
}

trait Sink {
  /** @param offset Position in target of the first byte to copy.
    * @param source Byte array to read bytes from.
    * @param index Position in source of the first byte to copy.
    * @param length Number of bytes to copy. */
  def put(offset: Long, source: Array[Byte], index: Int, length: Int): Unit
}

trait DataCache {
  def size: Long
  def written: Boolean
  def truncate(size: Long): Unit
  /** @param size Could also be implemented as Int. */
  def write(offset: Long, size: Long, source: Source): Unit
  /** @param size Could also be implemented as Int.
    * @return `false` if request exceeds available size. */
  def read(offset: Long, size: Long, sink: Sink): Boolean
  def close(): Unit
}

object Entry { def unapply[K,V](e: util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue) }

object MemCache {
  implicit class LongDecorator(val long: Long) extends AnyVal {
    def asInt: Int = { assert(long >= 0 && long <= Int.MaxValue, s"Illegal value $long"); long.toInt }
  }
}
class MemCache(available: AtomicLong) {
  import MemCache.LongDecorator

  protected val entries = new util.TreeMap[Long, Array[Byte]]()

  @tailrec
  private def tryAquire(size: Long): Boolean = {
    val avail = available.get()
    if (avail < size) false
    else if (available.compareAndSet(avail, avail - size)) true
    else tryAquire(size)
  }

  @tailrec
  private def release(size: Long): Unit = {
    val avail = available.get()
    if (!available.compareAndSet(avail, avail + size)) release(size)
  }

  /** @return `false` if not enough free memory available. */
  def write(offset: Long, data: Array[Byte]): Boolean = if (tryAquire(data.length)) {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(offset)).foreach { case Entry(storedPosition, stored) =>
      val overlap = storedPosition + stored.length - offset
      if (overlap > 0) {
        if (storedPosition != offset) entries.put(storedPosition, stored.dropRight(overlap.asInt))
        if (overlap > data.length) {
          entries.put(offset + data.length, stored.takeRight((overlap - data.length).asInt))
          release(data.length)
        } else release(overlap)
      }
    }

    // If necessary, trim higher entries.
    def trimHigher = Option(entries.higherEntry(offset)).map { case Entry(storedPosition, stored) =>
      val overlap = offset + data.length - storedPosition
      if (overlap > 0) {
        entries.remove(storedPosition)
        if (overlap < stored.length) {
          entries.put(storedPosition + overlap, stored.drop(overlap.asInt))
          release(overlap)
        } else release(stored.length)
        true
      } else false
    }
    while(trimHigher.contains(true)){/**/}

    // Store new entry.
    entries.put(offset, data)
    true
  } else false

}
