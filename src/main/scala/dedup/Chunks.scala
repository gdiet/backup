package dedup

import java.util.concurrent.atomic.AtomicLong

object CacheManager {
  val memoryUsed = new AtomicLong()
  var cacheLimit: Long = 100000000
}

object Chunk {
  def apply(position: Long, data: Array[Byte]): Chunk =
    if (CacheManager.memoryUsed.get >= CacheManager.cacheLimit) ???
    else new MemChunk(position, data)
}

sealed trait Chunk {
  assert(position >= 0, s"negative position $position")
  assert(size > 0, s"size $size not positive at $position")
  def position: Long
  def size: Int
  def data: Array[Byte]
  protected def sizeCheck(newSize: Int): Unit = {
    assert(newSize < size, s"newSize $newSize is not less than size $size")
    assert(newSize > 0, s"newSize $newSize is not greater than zero")
  }
  def left(newSize: Int): Chunk
  def right(newSize: Int): Chunk
  def drop(): Unit
}

class MemChunk(val position: Long, val data: Array[Byte]) extends Chunk {
  CacheManager.memoryUsed.addAndGet(size)
  override def size: Int = data.length
  override def left(newSize: Int): MemChunk = {
    sizeCheck(newSize)
    new MemChunk(position, data.take(newSize))
  }
  override def right(newSize: Int): MemChunk = {
    sizeCheck(newSize)
    new MemChunk(position, data.takeRight(newSize))
  }
  override def drop(): Unit = CacheManager.memoryUsed.addAndGet(-size)
}
