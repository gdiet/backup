// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.RandomAccessFile
import net.diet_rich.dedup.database.Print
import net.diet_rich.util.io._
import net.diet_rich.util.vals._
import net.diet_rich.util.Numbers

case class DataFile(file: RandomAccessFile, print: Print) { import DataFile._
  def writePrint = {
    file.seek(printPosition)
    file.writeLong(print)
  }
  def writeNewData(offsetInFileData: Position, bytes: Bytes, printOfBytes: Print): DataFile = {
    file.seek(startOfDataPosition + offsetInFileData.asSize)
    file.write(bytes)
    copy(print = print ^ printOfBytes)
  }
  def read(offsetInFileData: Position, bytes: Bytes): Size = {
    file.seek(startOfDataPosition + offsetInFileData.asSize)
    fillFrom(file, bytes.bytes, bytes.offset, bytes.length)
  }
//  def recalculatePrint: DataFile = {
//    val size = Numbers.toInt(file.length - startOfDataPosition.value)
//    val bytesToRead = new Array[Byte](size)
//    file.read(Position(0), bytesToRead, Position(0), Size(size))
//    print = calcDataPrint(Position(0), bytesToRead, Position(0), Size(size))
//  }
}

object DataFile {
  val fileNumberPosition = Position(0)
  val printPosition = Position(8)
  val startOfDataPosition = Position(16)

  def calcDataPrint(offsetInFileData: Position, bytes: Bytes): Print = {
    val startByte: Int = bytes.offset.intValue
    val endByte: Int = (bytes.offset + bytes.length).intValue
    val printOffset: Long = (offsetInFileData - bytes.offset).value + 1
    val array: Array[Byte] = bytes.bytes
    
    var print: Long = 0
    for (n <- startByte until endByte)
      print = print ^ (printOffset + n) * 5870203405204807807L * array(n)
    Print(print)
  }
}