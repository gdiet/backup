package dedup.cache

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

class ChannelCache(channel: SeekableByteChannel) {
  protected val entries = new util.TreeMap[Long, Int]()

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
}
