package dedup
package cache

import java.nio.file.Path
import dedup.cache.CacheBase

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For performance use a sparse file channel. */
class ChannelCache(temp: Path) extends LongCache with AutoCloseable:

  /** Assumes that the area to write is clear. */
  def write(offset: Long, data: Array[Byte]): Unit = ??? // FIXME don't forget mergeIfPossible

  /** Reads cached byte areas from this [[ChannelCache]].
    *
    * @param position position to start reading at.
    * @param size     number of bytes to read.
    *
    * @return A lazy list of (position, gapSize | byte array]). */
  def readData(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] = ??? // FIXME read memChunk sized chunks

  override def close(): Unit = ???
