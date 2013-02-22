// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.{File, RandomAccessFile}
import net.diet_rich.util.io.fillFrom
import DataFile2._

object DataFile2 {
  val headerBytes = 16
  
  def calcDataPrint(offsetInFileData: Int, bytes: Array[Byte], offsetInArray: Int, size: Int): Long = {
    var print: Long = 0L
    for (n <- 0 until size)
      print = print ^ (n + offsetInFileData + 1) * 5870203405204807807L * bytes(n + offsetInArray)
    print
  }
}

class DataFile2(dataFileNumber: Long, file: File, readonly: Boolean) {

  private var print: Long = 0L
  
  protected val randomAccessFile: RandomAccessFile = {
    if (readonly) {
      val fileAccess = new RandomAccessFile(file, "r")
      assume(dataFileNumber == fileAccess.readLong)
      fileAccess
    } else {
      val isNewFile = !file.exists()
      val fileAccess = new RandomAccessFile(file, "rw")
      if (isNewFile) {
        fileAccess.writeLong(dataFileNumber)
      } else {
        assume(dataFileNumber == fileAccess.readLong)
        fileAccess.seek(8)
        print = fileAccess.readLong
      }
      fileAccess
    }
  }
  
  def close = {
    if (!readonly) {
      randomAccessFile.seek(8)
      randomAccessFile.writeLong(print)
    }
    randomAccessFile.close
  }

  def writeNewData(offsetInFileData: Int, bytes: Array[Byte], offsetInArray: Int, size: Int, printOfBytes: Long): Unit = {
    randomAccessFile.seek(offsetInFileData + headerBytes)
    randomAccessFile.write(bytes, offsetInArray, size)
    print = print ^ printOfBytes
  }

  def eraseData(offsetInFileData: Int, size: Int): Unit =
    overwriteData(offsetInFileData, new Array[Byte](size), 0, size, 0L)
  
  def overwriteData(offsetInFileData: Int, bytesToWrite: Array[Byte], offsetInArray: Int, size: Int, printOfBytes: Long): Unit = {
    val bytesToRead = new Array[Byte](size)
    val numRead = read(offsetInFileData, bytesToRead, 0, size)
    print = print ^ calcDataPrint(offsetInFileData, bytesToRead, 0, numRead)
    writeNewData(offsetInArray, bytesToWrite, offsetInArray, size, printOfBytes)
  }

  def read(offsetInFileData: Int, bytesToRead: Array[Byte], offsetInArray: Int, size: Int): Int = {
    randomAccessFile.seek(offsetInFileData + headerBytes)
    fillFrom(randomAccessFile, bytesToRead, offsetInArray, size)
  }
  
}
