package dedup.cache

import java.util

trait CacheBase[T] {
  /** The methods are designed so no overlapping entries can occur. */
  protected var entries: util.NavigableMap[Long, T] = new util.TreeMap[Long, T]()

  /** Truncates the managed areas to the provided size. */
  def keep(newSize: Long)(implicit m: MemArea[T]): Unit = {
    // Remove higher entries (by keeping all strictly lower entries).
    entries = entries.headMap(newSize, false)
    // If necessary, trim highest remaining entry.
    Option(entries.lastEntry()).foreach { case Entry(storedPosition, storedArea) =>
      val distance = newSize - storedPosition
      if (distance < storedArea.length) entries.put(storedPosition, storedArea.take(distance))
    }
  }
}
