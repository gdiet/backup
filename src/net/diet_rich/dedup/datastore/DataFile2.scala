// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.{File, RandomAccessFile}
import net.diet_rich.util.io.fillFrom
import net.diet_rich.util.vals._
import DataFile2._
import net.diet_rich.util.Numbers

object DataFile2 {
  val headerBytes = 16
  
  def calcDataPrint(offsetInFileData: Position, bytes: Array[Byte], offsetInArray: IntPosition, size: IntSize): Long = {
    var print: Long = 0L
    for (n <- 0 until size.value)
      print = print ^ (n + offsetInFileData.value + 1) * 5870203405204807807L * bytes(n + offsetInArray.value)
    print
  }
}

// FIXME double-check!!!
class DataFile2(dataFileNumber: Long, file: File, mayCheckHeader: Boolean, readonly: Boolean) {

  private var print: Long = 0L
  
  protected val randomAccessFile: RandomAccessFile = {
    if (readonly) {
      val fileAccess = new RandomAccessFile(file, "r")
      assume((!mayCheckHeader) || dataFileNumber == fileAccess.readLong)
      fileAccess
    } else {
      val isNewFile = !file.exists()
      if (isNewFile) file.getParentFile.mkdirs
      val fileAccess = new RandomAccessFile(file, "rw")
      if (!isNewFile) {
        assume((!mayCheckHeader) || dataFileNumber == fileAccess.readLong)
        fileAccess.seek(8)
        print = fileAccess.readLong
      }
      fileAccess
    }
  }
  
  def close() = {
    if (!readonly) {
      randomAccessFile.seek(0)
      randomAccessFile.writeLong(dataFileNumber)
      randomAccessFile.writeLong(print)
    }
    randomAccessFile.close
  }

  def writeNewData(offsetInFileData: Position, bytes: Array[Byte], offsetInArray: IntPosition, size: IntSize, printOfBytes: Long): Unit = {
    randomAccessFile.seek(offsetInFileData.value + headerBytes)
    randomAccessFile.write(bytes, offsetInArray.value, size.value)
    print = print ^ printOfBytes
  }

  def eraseData(offsetInFileData: Position, size: IntSize): Unit =
    overwriteData(offsetInFileData, new Array[Byte](size.value), IntPosition(0), size, 0L)
  
  def overwriteData(offsetInFileData: Position, bytesToWrite: Array[Byte], offsetInArray: IntPosition, size: IntSize, printOfBytes: Long): Unit = {
    val bytesToRead = new Array[Byte](size.value)
    val numRead = read(offsetInFileData, bytesToRead, IntPosition(0), size)
    print = print ^ calcDataPrint(offsetInFileData, bytesToRead, IntPosition(0), numRead)
    writeNewData(offsetInFileData, bytesToWrite, offsetInArray, size, printOfBytes)
  }

  def read(offsetInFileData: Position, bytesToRead: Array[Byte], offsetInArray: IntPosition, size: IntSize): IntSize = {
    randomAccessFile.seek(offsetInFileData.value + headerBytes)
    IntSize(fillFrom(randomAccessFile, bytesToRead, offsetInArray.value, size.value))
  }
  
  def recalculatePrint = {
    val size = Numbers.toInt(randomAccessFile.length) - headerBytes
    val bytesToRead = new Array[Byte](size)
    read(Position(0), bytesToRead, IntPosition(0), IntSize(size))
    print = calcDataPrint(Position(0), bytesToRead, IntPosition(0), IntSize(size))
  }
  
}
