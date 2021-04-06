package dedup.cache

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

class MemCache(available: AtomicLong) {
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

//  def truncate(length: Long): Unit = {
//    // If necessary, trim floor entry.
//    Option(entries.floorEntry(length)).foreach { case Entry(storedPosition, stored) =>
//      val distance = length - storedPosition
//      if (distance < stored.length) {
//        entries.put(storedPosition, stored.take(distance.asInt))
//        release(stored.length - distance)
//      }
//    }
//    // Remove higher entries.
//    entries = entries.headMap(length, false)
//  }

  /** @return `false` if not enough free memory available and data is not cached. */
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
          false
        } else {
          release(stored.length)
          true
        }
      } else false
    }
    while(trimHigher.contains(true)){/**/}

    // Store new entry.
    entries.put(offset, data)
    true
  } else false

}
