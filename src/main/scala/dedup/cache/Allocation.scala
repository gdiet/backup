package dedup.cache

/** Keeps record of allocated ranges. Useful for keeping record of the zeros appended in a virtual file system
  * when the file system operation `truncate(newSize)` is called.
  *
  * Instances are not thread safe. */
class Allocation extends CacheBase[Long] {

  def clear(position: Long, size: Long): Unit = {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(position)).foreach { case Entry(storedPosition, storedSize) =>
      val distance = position - storedPosition
      if (distance < storedSize) entries.put(storedPosition, distance)
      val overlap = storedSize - distance
      if (overlap > size) entries.put(position + size, overlap - size)
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
  }

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit = {
    clear(position, size)
    entries.put(position, size)
  }

  /** @return Left: Holes. Right: Allocations. */
  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Long)]] =
    areasInSection(position, size)
}
