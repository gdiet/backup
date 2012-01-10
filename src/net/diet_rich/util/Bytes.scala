// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

case class Bytes(bytes: Array[Byte], length: Int, offset: Int = 0) {
  require(offset >= 0)
  require(length >= 0)
  require(bytes.length >= offset + length)
  
  def dropFirst(size: Int) : Bytes =
    copy(length = length - size, offset = offset + size)

  def keepFirst(size: Int) : Bytes = {
    require(size <= length)
    copy(length = size)
  }

  def keepAtMostFirst(size: Int) : Bytes = {
    copy(length = math.min(size, length))
  }

  def apply(position: Int) : Byte = {
    require(position <= length)
    require(position + offset <= bytes.length)
    bytes(position + offset)
  }
  
  def store(long : Long) : Bytes = {
    require(length == 8)
    bytes(0) = (long >>  0).toByte
    bytes(1) = (long >>  8).toByte
    bytes(2) = (long >> 16).toByte
    bytes(3) = (long >> 24).toByte
    bytes(4) = (long >> 32).toByte
    bytes(5) = (long >> 40).toByte
    bytes(6) = (long >> 48).toByte
    bytes(7) = (long >> 56).toByte
    this
  }
  
  def toLong : Long = {
    require(length == 8)
    (bytes(0).toLong & 0xff) <<  0 |
    (bytes(1).toLong & 0xff) <<  8 |
    (bytes(2).toLong & 0xff) << 16 |
    (bytes(3).toLong & 0xff) << 24 |
    (bytes(4).toLong & 0xff) << 32 |
    (bytes(5).toLong & 0xff) << 40 |
    (bytes(6).toLong & 0xff) << 48 |
    (bytes(7).toLong & 0xff) << 56
  }

  def filled : Boolean = length == bytes.length
  
  override def toString : String = "Bytes(%s)".format(bytes.map("%02x".format(_)).mkString(" "))
}

object Bytes {
  def apply(bytes: Array[Byte]) : Bytes = Bytes(bytes, bytes.length)
  def apply(size: Int) : Bytes = Bytes(new Array[Byte](size))
  def forLong(long: Long) : Bytes = Bytes(8).store(long)
}
