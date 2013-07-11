// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.vals

import net.diet_rich.util.vals._

case class Print(value: Long) extends LongValue {
  def ^(other: Print): Print = copy(value ^ other.value)
}

object Print {
  val size = 8192
  def apply(bytes: Bytes): Print = apply(bytes.data, bytes.offset, bytes.length)
  def apply(bytes: Array[Byte], offset: Int, len: Int): Print = {
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc update (bytes, offset, len)
    adler update (bytes, offset, len)
    Print(adler.getValue << 32 | crc.getValue)
  }
}
