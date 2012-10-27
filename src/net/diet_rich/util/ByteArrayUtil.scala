// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object ByteArrayUtil {

  def readLong(data: Array[Byte], start: Int): Long = {
    assume(start <= data.length - 8, "read position %s must not be behind size-8 ($%s)" format (start, data.length - 8))
    (data(start+0).toLong & 0xff) <<  0 |
    (data(start+1).toLong & 0xff) <<  8 |
    (data(start+2).toLong & 0xff) << 16 |
    (data(start+3).toLong & 0xff) << 24 |
    (data(start+4).toLong & 0xff) << 32 |
    (data(start+5).toLong & 0xff) << 40 |
    (data(start+6).toLong & 0xff) << 48 |
    (data(start+7).toLong & 0xff) << 56
  }

  def writeLong(data: Array[Byte], start: Int, value: Long) = {
    assume(start <= data.length - 8, "write position %s must not be behind size-8 ($%s)" format (start, data.length - 8))
    data(start+0) = (value >>  0).toByte
    data(start+1) = (value >>  8).toByte
    data(start+2) = (value >> 16).toByte
    data(start+3) = (value >> 24).toByte
    data(start+4) = (value >> 32).toByte
    data(start+5) = (value >> 40).toByte
    data(start+6) = (value >> 48).toByte
    data(start+7) = (value >> 56).toByte
  }

  def writeLongs(data: Array[Byte], start: Int, values: Long*) =
    values.zipWithIndex.foreach{case (value, index) => writeLong(data, start + index * 8, value)}
  
}