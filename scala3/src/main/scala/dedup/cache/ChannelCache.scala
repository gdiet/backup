package dedup
package cache

import java.nio.file.Path
import dedup.cache.CacheBase

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For performance use a sparse file channel. */
class ChannelCache(temp: Path) extends CacheBase[Int] with AutoCloseable:
  
  override protected def length(m: Int): Long = m
  override protected def drop  (m: Int, distance: Long): Int        = m - distance.asInt
  override protected def keep  (m: Int, distance: Long): Int        = distance.asInt
  override protected def split (m: Int, distance: Long): (Int, Int) = (distance.asInt, m - distance.asInt)

  /** Assumes that the area to write is clear. */
  def write(offset: Long, data: Array[Byte]): Unit = ???

  /** TODO document */
  def readData(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] = ???

  override def close(): Unit = ???
