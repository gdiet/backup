// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.data

import net.diet_rich.util.ASSUME

final case class Bytes(bytes: Array[Byte], length: Long, offset: Long = 0) {
  // since the unterlying Array is restricted to Int offset and length,
  // offset and length of Bytes is restricted in the same way.
  ASSUME(offset >= 0, "offset " + offset + " may not be negative")
  ASSUME(offset <= Int.MaxValue, "offset " + offset + " must be less than Int.MaxValue")
  ASSUME(length >= 0, "length " + length + " may not be negative")
  ASSUME(length <= Int.MaxValue, "length " + length + " must be less than Int.MaxValue")
  ASSUME(bytes.length >= offset + length, "bytes.length " + bytes.length + " must be at least offset+length " + (offset+length))

  protected def intOff = offset.toInt
  protected def intLen = length.toInt
  
  def copyFrom(other: Bytes, offset: Long = 0) : Bytes = {
    System.arraycopy(other bytes, other.offset toInt, bytes, (this.offset + offset) toInt, other.length toInt)
    this
  }

  def copyTo(other: Bytes, offset: Long = 0) : Bytes = {
    System.arraycopy(bytes, this.offset toInt, other bytes, (other.offset + offset) toInt, this.length toInt)
    other
  }

  def extendMax: Bytes = extend(bytes.length - offset)

  def extend(size: Long) : Bytes = copy(length = size)

  def dropFirst(size: Long) : Bytes =
    copy(length = length - size, offset = offset + size)

  def keepFirst(size: Long) : Bytes = {
    require(size <= length)
    copy(length = size)
  }

  def keepAtMostFirst(size: Long) : Bytes = {
    copy(length = math.min(size, length))
  }

  def apply(position: Long) : Byte = {
    require(position <= length)
    require(position + offset <= bytes.length)
    bytes((position + offset) toInt)
  }

  // Note: java.io.DataOuput writes big-endian, but these
  // methods stick to the more common little-endian.
  def write(long: Long) : Bytes = {
    bytes(intOff+0) = (long >>  0).toByte
    bytes(intOff+1) = (long >>  8).toByte
    bytes(intOff+2) = (long >> 16).toByte
    bytes(intOff+3) = (long >> 24).toByte
    bytes(intOff+4) = (long >> 32).toByte
    bytes(intOff+5) = (long >> 40).toByte
    bytes(intOff+6) = (long >> 48).toByte
    bytes(intOff+7) = (long >> 56).toByte
    this
  }
  
  def store(long: Long) : Bytes = write (long) dropFirst 8

  def reader = new BytesReader(this)
  
  def readLong = reader.readLong
  
  def filled : Boolean = length == bytes.length

  def toArray : Array[Byte] = {
    val result = new Array[Byte](length toInt)
    System.arraycopy(bytes, offset toInt, result, 0, length toInt)
    result
  }
  
  override def toString : String = "Bytes(%s/%s)(%s)".format(offset, length, bytes.map("%02x".format(_)).mkString(" "))
}


object Bytes {
  def apply(bytes: Array[Byte]) : Bytes = Bytes(bytes, bytes.length)
  def apply(size: Long) : Bytes = Bytes(new Array[Byte](size toInt), size)
  def forLong(long: Long) : Bytes = Bytes(8).write(long)
}


class BytesReader(private[Bytes] bytes: Bytes) {
  var length = bytes.length.toInt
  var offset = bytes.offset.toInt
  var data = bytes.bytes

  def readLong : Long = {
    ASSUME(length >= 8, "length " + length + " must be at least 8 to read a Long")
    val result = 
      (data(offset+0).toLong & 0xff) <<  0 |
      (data(offset+1).toLong & 0xff) <<  8 |
      (data(offset+2).toLong & 0xff) << 16 |
      (data(offset+3).toLong & 0xff) << 24 |
      (data(offset+4).toLong & 0xff) << 32 |
      (data(offset+5).toLong & 0xff) << 40 |
      (data(offset+6).toLong & 0xff) << 48 |
      (data(offset+7).toLong & 0xff) << 56
    length = length - 8
    offset = offset + 8
    result
  }
}
