package dedup.cache

import java.util

/** Keeps record of allocated ranges. Useful for keeping record of the zeros appended in a virtual file system
  * when the file system operation `truncate(newSize)` is called.
  *
  * Instances are not thread safe. */
class Allocation {
  /** The methods are designed so no overlapping entries can occur. */
  protected var entries: util.NavigableMap[Long, Long] = new util.TreeMap[Long, Long]()

  /** Truncates the allocated ranges to the provided size. */
  def truncate(newSize: Long): Unit = {
    // Remove higher entries (by keeping all strictly lower entries).
    entries = entries.headMap(newSize, false)
    // If necessary, trim highest entry.
    Option(entries.lastEntry()).foreach { case Entry(storedPosition, storedSize) =>
      val distance = newSize - storedPosition
      if (distance < storedSize) entries.put(storedPosition, distance)
    }
  }

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit = {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(position)).foreach { case Entry(storedPosition, storedSize) =>
      val distance = position - storedPosition
      if (distance < storedSize) entries.put(storedPosition, distance)
    }

    /** If necessary, trim higher entries.
      * @return `true` if trimming needs to be continued. */
    def trimHigher(): Boolean = Option(entries.higherEntry(position)).exists { case Entry(storedPosition, storedSize) =>
      val overlap = position + size - storedPosition
      if (overlap <= 0) false else {
        entries.remove(storedPosition)
        if (overlap >= storedSize) true else {
          entries.put(storedPosition + overlap, storedSize - overlap)
          false
        }
      }
    }
    while(trimHigher()){/**/}

    // Store new entry.
    entries.put(position, size)
  }
}
