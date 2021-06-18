package dedup
package cache

import dedup.cache.CacheBase

/** Keeps record of allocated ranges. Used to keep record of the zeros appended
  * when the file system operation `truncate(newSize)` is called. */
class Allocation extends LongCache:

  /** Allocates a range. */
  def allocate(position: Long, size: Long): Unit =
    entries.put(position, size)
    mergeIfPossible(position)

  /** Reads allocated zero byte areas from this [[Allocation]].
    *
    * @param position position to start reading at.
    * @param size     number of bytes to read.
    *
    * @return A lazy list of (offset, gapSize | byte array]) where offset is relative to `position`. */
  def readData(position: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] =
    read(position, size).flatMap {
      case position -> Right(size) =>
        val end = position + size
        Range.Long(position, end, memChunk)
          .map(pos => pos -> Right(new Array[Byte](math.min(end - pos, memChunk).asInt)))
      case position -> Left(hole) => Seq(position -> Left(hole))
    }
