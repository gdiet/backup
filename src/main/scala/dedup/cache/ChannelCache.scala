package dedup.cache

import dedup.{memChunk, scalaUtilChainingOps}

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

/** Caches in a file byte arrays with positions, where the byte arrays are not necessarily contiguous.
  * For best performance use a sparse file channel.
  *
  * Instances are not thread safe. */
class ChannelCache(channel: SeekableByteChannel) extends CacheBase[Int] {
  /** Truncates the allocated ranges to the provided size. */
  override def keep(newSize: Long)(implicit m: MemArea[Int]): Unit = {
    super.keep(newSize)
    if (channel.size() > newSize) channel.truncate(newSize)
  }

  def write(offset: Long, data: Array[Byte]): Unit = {
    // If necessary, trim floor entry.
    Option(entries.floorEntry(offset)).foreach { case Entry(storedPosition, storedLength) =>
      val distance = offset - storedPosition
      if (distance < storedLength) entries.put(storedPosition, distance.asInt)
    }

    // If necessary, trim higher entries.
    def trimHigher = Option(entries.higherEntry(offset)).map { case Entry(storedPosition, storedLength) =>
      val overlap = offset + data.length - storedPosition
      if (overlap > 0) {
        entries.remove(storedPosition)
        if (overlap < storedLength) {
          entries.put(storedPosition + overlap, (storedLength - overlap).asInt)
          false
        } else true
      } else false
    }
    while(trimHigher.contains(true)){/**/}

    // Store new entry.
    entries.put(offset, data.length)
    channel.position(offset)
    channel.write(ByteBuffer.wrap(data, 0, data.length))
  }

  def read(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, Array[Byte])]] = {
    areasInSection(entries, position, size).map {
      case Left(left) => Left(left)
      case Right(localPos -> localSize) =>
        channel.position(localPos)
        Right(localPos -> new Array[Byte](localSize).tap { data =>
          while (channel.read(ByteBuffer.wrap(data)) > 0) {/**/}
        })
    }
  }
}
