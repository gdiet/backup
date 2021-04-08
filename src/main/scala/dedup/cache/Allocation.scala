package dedup.cache

/** Keeps record of allocated ranges. Useful for keeping record of the zeros appended in a virtual file system
  * when the file system operation `truncate(newSize)` is called.
  *
  * Instances are not thread safe. */
class Allocation(implicit val m: MemArea[Long]) extends CacheBase[Long] {

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit = {
    clear(position, size)
    entries.put(position, size)
  }

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Long)]] =
    areasInSection(position, size)
}
