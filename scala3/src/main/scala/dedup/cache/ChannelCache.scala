package dedup
package cache

import java.nio.file.Path
import dedup.cache.CacheBase

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For performance use a sparse file channel. */
class ChannelCache(temp: Path) extends CacheBase[Long] with AutoCloseable:

  // FIXME used two or three times!
  override protected def length(m: Long): Long = m
  override protected def merge (m: Long, n       : Long ): Option[Long] = Some(m + n)
  override protected def drop  (m: Long, distance: Long): Long         = m - distance
  override protected def keep  (m: Long, distance: Long): Long         = distance
  override protected def split (m: Long, distance: Long): (Long, Long)  = (distance, m - distance)

  /** Assumes that the area to write is clear. */
  def write(offset: Long, data: Array[Byte]): Unit = ??? // FIXME don't forget mergeIfPossible

  /** TODO document */
  def readData(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] = ??? // FIXME read memChunk sized chunks

  override def close(): Unit = ???
