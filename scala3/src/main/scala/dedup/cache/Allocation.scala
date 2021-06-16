package dedup
package cache

import dedup.cache.CacheBase

/** Keeps record of allocated ranges. Used to keep record of the zeros appended
  * when the file system operation `truncate(newSize)` is called. */
class Allocation extends CacheBase[Long]:

  override protected def length(m: Long): Long = m
  override protected def drop  (m: Long, distance: Long): Long         = m - distance
  override protected def keep  (m: Long, distance: Long): Long         = distance
  override protected def split (m: Long, distance: Long): (Long, Long) = (distance, m-distance)

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit = entries.put(position, size)
