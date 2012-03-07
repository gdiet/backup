// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.data

case class Bytes(bytes: Array[Byte], length: Long, offset: Long = 0) {
  // since the unterlying Array is restricted to Int offset and length,
  // offset and length of Bytes is restricted in the same way.
  // TODO use ASSUME
  require(offset >= 0)
  require(offset <= Int.MaxValue)
  require(length >= 0)
  require(length <= Int.MaxValue)
  require(bytes.length >= offset + length)

  lazy val intOff = offset.toInt
  lazy val intLen = length.toInt
  
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

  def store(long : Long) : Bytes = {
    require(length == 8)
    storeIn(long)
  }

  def storeIn(long : Long) : Bytes = {
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

  def toLong : Long = {
    require(length == 8)
    longFrom
  }

  def longFrom : Long = {
    (bytes(intOff+0).toLong & 0xff) <<  0 |
    (bytes(intOff+1).toLong & 0xff) <<  8 |
    (bytes(intOff+2).toLong & 0xff) << 16 |
    (bytes(intOff+3).toLong & 0xff) << 24 |
    (bytes(intOff+4).toLong & 0xff) << 32 |
    (bytes(intOff+5).toLong & 0xff) << 40 |
    (bytes(intOff+6).toLong & 0xff) << 48 |
    (bytes(intOff+7).toLong & 0xff) << 56
  }

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
  def forLong(long: Long) : Bytes = Bytes(8).storeIn(long)
}
