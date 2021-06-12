package dedup
package cache

import dedup.cache.CacheBase

/** Keeps record of allocated ranges. Used to keep record of the zeros appended
  * when the file system operation `truncate(newSize)` is called. */
class Allocation extends CacheBase[Long]:

  extension(m: Long)
    override protected def length: Long = m
    override protected def dropped: Unit = {/**/}
    override protected def drop (distance: Long): Long =
      require(distance < length && distance > 0, s"Distance: $distance")
      m - distance
    override protected def keep (distance: Long): Long =
      require(distance < length && distance > 0, s"Distance: $distance")
      distance
    override protected def split(distance: Long): (Long, Long) =
      require(distance < length && distance > 0, s"Distance: $distance")
      (distance, m-distance)

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit = entries.put(position, size)

  def read(position: Long, size: Long): LazyList[(Long, Either[Long, Long])] =
    areasInSection(position, size)
