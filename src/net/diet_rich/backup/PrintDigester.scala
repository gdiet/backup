// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.io._
import net.diet_rich.util.ImmutableBytes

class PrintResult(val print: Long, val data: ImmutableBytes)

trait PrintDigester {
  def zeroBytePrint: Long
  /** If the ImmutableBytes object is not "full", the input has been read up to EOI. */
  def print(reader: Reader): PrintResult
}

object CrcAdler8192 extends PrintDigester {
  val zeroBytePrint: Long = print(emptyReader).print

  override def print(reader: Reader): PrintResult = {
    val data = new Array[Byte](8192)
    val size = fillFrom(reader, data, 0, 8192)
    val print = CrcAdler.print(data, 0, size)
    new PrintResult(print, ImmutableBytes(data, size))
  }
}

object CrcAdler {
  def print(bytes: Array[Byte], offset: Int, length: Int): Long = {
    assume (length > 0, "length %s must be positive but is %s" format length)
    assume (offset + length <= bytes.length, "offset %s + length %s must be less or equal byte array length %s" format (offset, length, bytes.length))
    
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc.update(bytes, offset, length)
    adler.update(bytes, offset, length)
    adler.getValue << 32 | crc.getValue
  }
}
