// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.io._
import net.diet_rich.util.vals._

trait PrintDigester {
  def calculatePrint(reader: SeekReader): Print
  def filterPrint[ReturnType](source: ByteSource)(processor: ByteSource => ReturnType): (Print, ReturnType)
}

trait CrcAdler8192 extends PrintDigester { import CrcAdler8192._
  private val area = Size(8192)
  
  def calculatePrint(reader: SeekReader): Print = {
    val data = new Array[Byte](area.intValue)
    val size = fillFrom(reader, data, Position(0), area)
    reader.seek(Position(0))
    calculate(data, 0, size.intValue)
  }
  
  def filterPrint[ReturnType](source: ByteSource)(processor: ByteSource => ReturnType): (Print, ReturnType) = {
    val data = new Array[Byte](area.intValue)
    val size = fillFrom(source, data, Position(0), area)
    val print = calculate(data, 0, size.intValue)
    val partitionedInput = source.prependArray(data, 0, size.intValue)
    (print, processor(partitionedInput))
  }
}

object CrcAdler8192 {
  def zeroBytesPrint = calculate(Array(), 0, 0)
  
  def calculate(bytes: Array[Byte], offset: Int, length: Int): Print = {
    assume (length > -1, s"length must not be negative but is $length")
    assume (offset + length <= bytes.length, s"offset $offset + length $length must be less or equal byte array length ${bytes.length}")
    
    val crc = new java.util.zip.CRC32
    val adler = new java.util.zip.Adler32
    crc.update(bytes, offset, length)
    adler.update(bytes, offset, length)
    Print(adler.getValue << 32 | crc.getValue)
  }
}
