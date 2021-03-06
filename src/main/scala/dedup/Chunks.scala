package dedup

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.util.concurrent.atomic.AtomicLong

object CacheManager {
  val memoryUsed = new AtomicLong()
  // Use only 70% of theoretically available memory - a helpful fix that I don't really understand
  var cacheLimit: Long = (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10
}

object CacheChunk {
  def apply(position: Long, data: Array[Byte], channel: => SeekableByteChannel): CacheChunk =
    if (CacheManager.memoryUsed.get < CacheManager.cacheLimit) new MemChunk(position, data)
    else {
      val buffer = ByteBuffer.wrap(data)
      channel.position(position)
      while (buffer.hasRemaining) channel.write(buffer)
      new FileChunk(position, data.length, channel)
    }
}

sealed trait CacheChunk {
  assert(position >= 0, s"negative position $position")
  assert(size > 0, s"size $size not positive at $position")
  def position: Long
  def end: Long = position + size
  def size: Int
  def data: Array[Byte]
  protected def sizeCheck(newSize: Int): Unit = {
    assert(newSize < size, s"newSize $newSize is not less than size $size")
    assert(newSize > 0, s"newSize $newSize is not greater than zero")
  }
  def left(newSize: Int): CacheChunk
  def right(newSize: Int): CacheChunk
  def drop(): Unit
}

class MemChunk(override val position: Long, override val data: Array[Byte]) extends CacheChunk {
  CacheManager.memoryUsed.addAndGet(size)
  override def size: Int = data.length
  override def left(newSize: Int): MemChunk = {
    sizeCheck(newSize)
    new MemChunk(position, data.take(newSize))
  }
  override def right(newSize: Int): MemChunk = {
    sizeCheck(newSize)
    new MemChunk(position + size - newSize, data.takeRight(newSize))
  }
  override def drop(): Unit = CacheManager.memoryUsed.addAndGet(-size)
}

class FileChunk(override val position: Long, override val size: Int, channel: SeekableByteChannel) extends CacheChunk {
  override def data: Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(size)
    channel.position(position)
    while (buffer.hasRemaining) channel.read(buffer)
    buffer.array()
  }
  override def left(newSize: Int): FileChunk = {
    sizeCheck(newSize)
    new FileChunk(position, newSize, channel)
  }
  override def right(newSize: Int): FileChunk = {
    sizeCheck(newSize)
    new FileChunk(position + size - newSize, newSize, channel)
  }
  override def drop(): Unit = {}
}
