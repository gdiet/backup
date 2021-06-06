package dedup
package cache

import java.nio.file.Path
import dedup.cache.CacheBase

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For performance use a sparse file channel. */
class ChannelCache(temp: Path) extends CacheBase[Int] with AutoCloseable:
  
  extension(m: Int)
    override protected def length: Long = m
    override protected def dropped: Unit = {/**/}
    override protected def drop (distance: Long): Int =
      require(distance < length && distance > 0, s"Distance: $distance")
      m - distance.asInt
    override protected def keep (distance: Long): Int =
      require(distance < length && distance > 0, s"Distance: $distance")
      distance.asInt
    override protected def split(distance: Long): (Int, Int) =
      require(distance < length && distance > 0, s"Distance: $distance")
      (distance.asInt, m - distance.asInt)

  /** Assumes that the area to write is clear. */
  def write(offset: Long, data: Array[Byte]): Unit = ???

  override def close(): Unit = ???
