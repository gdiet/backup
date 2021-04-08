package dedup.cache

import dedup.{memChunk, scalaUtilChainingOps}

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For best performance use a sparse file channel.
  *
  * Instances are not thread safe. */
class ChannelCache(channel: SeekableByteChannel)(implicit val m: MemArea[Int]) extends CacheBase[Int] {
  /** Truncates the allocated ranges to the provided size. */
  override def keep(newSize: Long): Unit = {
    super.keep(newSize)
    if (channel.size() > newSize) channel.truncate(newSize)
  }

  /** Assumes that the area to write is clear. */
  def write(offset: Long, data: Array[Byte]): Unit = {
    entries.put(offset, data.length)
    channel.position(offset)
    channel.write(ByteBuffer.wrap(data, 0, data.length))
  }

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] = {
    areasInSection(position, size).map {
      case Left(left) => Left(left)
      case Right(localPos -> localSize) =>
        channel.position(localPos)
        Right(localPos -> new Array[Byte](localSize).tap { data =>
          while (channel.read(ByteBuffer.wrap(data)) > 0) {/**/}
        })
    }
  }
}
