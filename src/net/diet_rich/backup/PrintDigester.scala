// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.io._
import net.diet_rich.util.ImmutableBytes

class PrintResult(val print: Long, val data: ImmutableBytes)

trait PrintDigester {
  def zeroBytePrint: Long
  /** If the Bytes object is not "full", the input has been read up to EOI. */
  def print(reader: Reader): PrintResult
}

object CrcAdler8192 extends PrintDigester {
  val zeroBytePrint: Long = print(emptyReader).print

  override def print(reader: Reader): PrintResult = {
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    val data = new Array[Byte](8192)
    val size = fillFrom(reader, data, 0, 8192)
    crc.update(data, 0, size)
    adler.update(data, 0, size)
    val print = adler.getValue << 32 | crc.getValue
    new PrintResult(print, ImmutableBytes(data, size))
  }
}
