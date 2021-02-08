package dedup2

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.util.concurrent.atomic.AtomicLong

case class MemCached(override val start: Long, private val data: Array[Byte]) extends Cached {
  override def cacheSize: Long = data.length + 64
  override def end: Long = start + data.length
  override def read(from: Long, to: Long): Array[Byte] = {
    require(from >= start, s"$from !>= $start")
    require(to <= end, s"$to !<= $end")
    data.slice((from - start).toInt, (to-start).toInt)
  }
  override def take(size: Long): MemCached = {
    require(size < data.length, s"$size !< ${data.length}")
    copy(data = data.take(size.toInt))
  }
  override def drop(size: Long): MemCached = {
    require(size < data.length, s"$size !< ${data.length}")
    copy(data = data.drop(size.toInt))
  }
  override def toString: String = s"MemCached($start, size ${data.length}, data ${data.take(10).toSeq}...)"
}

case class FileCached(override val start: Long, size: Long, channel: SeekableByteChannel) extends Cached {
  override def cacheSize: Long = 128
  override def end: Long = start + size
  override def read(from: Long, to: Long): Array[Byte] = {
    require(from >= start, s"$from !>= $start")
    require(to <= end, s"$to !<= $end")
    val buffer = java.nio.ByteBuffer.allocate((to-from).toInt)
    channel.position(from)
    while (buffer.hasRemaining) channel.read(buffer)
    buffer.array()
  }
  override def take(size: Long): FileCached = {
    require(size < this.size, s"$size !< ${this.size}")
    copy(size = size)
  }
  override def drop(size: Long): FileCached = {
    require(size < this.size, s"$size !< ${this.size}")
    copy(start = start + size, size = this.size - size)
  }
  override def toString: String = s"FileCached($start, size $size, data ${read(start, math.min(10, size)).toSeq}...)"
}

trait Cached {
  def start: Long
  def end: Long
  /** Not all implementations are thread safe, must be synchronized externally. */
  def read(from: Long, to: Long): Array[Byte]
  def take(size: Long): Cached
  def drop(size: Long): Cached
  def cacheSize: Long
}
object Cached {
  val cacheLimit: Long = math.max(16000000, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val cacheUsed = new AtomicLong(0)

  /** Not thread safe, must be synchronized externally. */
  def apply(start: Long, data: Array[Byte], channel: => SeekableByteChannel): Cached =
    if (cacheUsed.get() < cacheLimit) MemCached(start, data) else {
      val buffer = ByteBuffer.wrap(data)
      channel.position(start)
      while (buffer.hasRemaining) channel.write(buffer)
      FileCached(start, data.length, channel)
    }
}
