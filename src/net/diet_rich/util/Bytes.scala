package net.diet_rich.util

case class Bytes(data: Array[Byte], offset: Long, size: Long) {
  ASSUME (offset >= 0, "offset must be positive but is %s" format offset)
  ASSUME (size > 0, "size must be positive but is %s" format size)
  ASSUME (offset + size <= Int.MaxValue, "offset + size must be an Integer value but is %s" format offset)
  
  def intOffset = offset toInt
  def intSize = size toInt

  def dropFirst(length: Long) = copy(offset = offset + length, size = size - length)

  def setSize(length: Long) = copy(size = length)

  def maxSize: Long = data.length

  def copyFrom(other: Bytes, offset: Long = 0) : Bytes = {
    ASSUME(other.size + offset <= size, "failed: other.size + offset <= size: %s + %s <= %s" format (other.size, offset, size))
    System.arraycopy(other data, other intOffset, data, (this.offset + offset) toInt, other intSize)
    this
  }

  def copyOfBytes : Array[Byte] = {
    val result = new Array[Byte](intSize)
    System.arraycopy(data, intOffset, result, 0, intSize)
    result
  }
  
  def readLong(position: Long) : Long = {
    ASSUME(position <= size-8, "read position %s must not be behind size-8 ($%s)" format (position, size-8))
    val start = (offset + position).toInt
    (data(start+0).toLong & 0xff) <<  0 |
    (data(start+1).toLong & 0xff) <<  8 |
    (data(start+2).toLong & 0xff) << 16 |
    (data(start+3).toLong & 0xff) << 24 |
    (data(start+4).toLong & 0xff) << 32 |
    (data(start+5).toLong & 0xff) << 40 |
    (data(start+6).toLong & 0xff) << 48 |
    (data(start+7).toLong & 0xff) << 56
  }

  def writeLong(position: Long, value: Long) : Bytes = {
    ASSUME(position <= size-8, "write position %s must not be behind size-8 ($%s)" format (position, size-8))
    val start = (offset + position).toInt
    data(start+0) = (value >>  0).toByte
    data(start+1) = (value >>  8).toByte
    data(start+2) = (value >> 16).toByte
    data(start+3) = (value >> 24).toByte
    data(start+4) = (value >> 32).toByte
    data(start+5) = (value >> 40).toByte
    data(start+6) = (value >> 48).toByte
    data(start+7) = (value >> 56).toByte
    this
  }
  
}

object Bytes {
  def apply(size: Long) : Bytes = Bytes(new Array[Byte](size toInt), 0, size)
  def apply(data: Array[Byte]) : Bytes = Bytes(data, 0, data.length)
}