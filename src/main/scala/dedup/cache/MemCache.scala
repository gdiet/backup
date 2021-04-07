package dedup.cache

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

/** Caches in memory byte arrays with positions, where the byte arrays are not necessarily contiguous.
  *
  * Instances are not thread safe. */
class MemCache(availableMem: AtomicLong) {
  protected var entries: util.NavigableMap[Long, Array[Byte]] = new util.TreeMap[Long, Array[Byte]]()

  @tailrec
  private def tryAquire(size: Long): Boolean = {
    val avail = availableMem.get()
    if (avail < size) false
    else if (availableMem.compareAndSet(avail, avail - size)) true
    else tryAquire(size)
  }

  @tailrec
  private def release(size: Long): Unit = {
    val avail = availableMem.get()
    if (!availableMem.compareAndSet(avail, avail + size)) release(size)
  }

  /** Truncates the cached data to the provided size. */
  def keep(newSize: Long): Unit = {
    // Remove higher entries (by keeping all strictly lower entries).
    entries = entries.headMap(newSize, false)
    // If necessary, trim highest entry.
    Option(entries.lastEntry()).foreach { case Entry(storedPosition, stored) =>
      val distance = newSize - storedPosition
      if (distance < stored.length) {
        entries.put(storedPosition, stored.take(distance.asInt))
        release(stored.length - distance)
      }
    }
  }

  /** @return `false` if not enough free memory available and data is not cached. */
  def write(position: Long, data: Array[Byte]): Boolean = if (tryAquire(data.length)) {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(position)).foreach { case Entry(storedPosition, stored) =>
      val overlap = storedPosition + stored.length - position
      if (overlap > 0) {
        if (storedPosition != position) entries.put(storedPosition, stored.dropRight(overlap.asInt))
        if (overlap > data.length) {
          entries.put(position + data.length, stored.takeRight((overlap - data.length).asInt))
          release(data.length)
        } else release(overlap)
      }
    }

    // If necessary, trim higher entries.
    def trimHigher = Option(entries.higherEntry(position)).map { case Entry(storedPosition, stored) =>
      val overlap = position + data.length - storedPosition
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
    entries.put(position, data)
    true
  } else false

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] = {
    MemAreaSection(entries, position, size)
  }
}
