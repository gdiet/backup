// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.io._

trait PrintDigester {
  def calculatePrintAndReset(reader: SeekReader): Print
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Print, ReturnType)
}

trait CrcAdler8192 extends PrintDigester {
  private val area = 8192
  
  def calculatePrintAndReset(reader: SeekReader): Print = {
    val data = new Array[Byte](area)
    val size = fillFrom(reader, data, 0, area)
    calculatePrint(data, 0, size)
  }
  
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Print, ReturnType) = {
    val data = new Array[Byte](area)
    val size = fillFrom(input, data, 0, area)
    val print = calculatePrint(data, 0, size)
    val paritionedInput = prependArray(data, 0, size, input)
    (print, reader(paritionedInput))
  }
  
  private def calculatePrint(bytes: Array[Byte], offset: Int, length: Int): Print = {
    assume (length > -1, "length must not be negative but is %s" format length)
    assume (offset + length <= bytes.length, "offset %s + length %s must be less or equal byte array length %s" format (offset, length, bytes.length))
    
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc.update(bytes, offset, length)
    adler.update(bytes, offset, length)
    Print(adler.getValue << 32 | crc.getValue)
  }
}

object CrcAdler8192 {
  def zeroBytesPrint = new CrcAdler8192{}.calculatePrint(Array(), 0, 0)
}
