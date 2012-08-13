// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.io._

class PrintResult(val print: Long, val data: Array[Byte], val size: Int)

trait PrintDigester {
  def zeroBytePrint: Long
  def print(reader: Reader): PrintResult
}

object CrcAdler8192 extends PrintDigester {
  val zeroBytePrint: Long = print(emptyReader).print

  def print(reader: Reader): PrintResult = {
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    val data = new Array[Byte](8192)
    val size = fillFrom(reader, data, 0, 8192)
    crc.update(data, 0, size)
    adler.update(data, 0, size)
    val print = adler.getValue << 32 | crc.getValue
    new PrintResult(print, data, size)
  }
}
