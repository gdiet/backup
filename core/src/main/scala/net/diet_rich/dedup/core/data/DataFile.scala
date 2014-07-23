// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.data

import java.io.{IOException, File, RandomAccessFile}

import net.diet_rich.dedup.core.values.{Size, Bytes}
import net.diet_rich.dedup.util.init

object DataFile {
  val headerBytes = 16

  def calcDataPrint(offsetInFileData: Long, bytes: Bytes): Long = {
    val time = System.nanoTime()
    import bytes._
    // Performance optimized.
    var print: Long = 0L
    for (n <- 0 until length)
      print = print ^ (n + 1 + offsetInFileData) * 5870203405204807807L * data(n + offset)
    println(System.nanoTime() - time)
    print
  }
}

class DataFile(dataFileNumber: Long, file: File, readonly: Boolean) {
  import DataFile._

  private var print: Long = 0L

  private val fileAccess: RandomAccessFile =
    if (readonly)
      openAndCheckHeader("r")
    else if (file exists())
      openAndCheckHeader("rw")
    else {
      file.getParentFile mkdirs()
      init(new RandomAccessFile(file, "rw")){_ writeLong dataFileNumber}
    }

  private def openAndCheckHeader(accessType: String): RandomAccessFile =
    init(new RandomAccessFile(file, accessType)) { fileAccess =>
      val fileNumberRead = fileAccess.readLong
      print = fileAccess.readLong
      if (dataFileNumber != fileNumberRead) {
        fileAccess close()
        throw new IOException(s"Data file number read $fileNumberRead is not $dataFileNumber")
      }
    }

  def close() = {
    if (!readonly) {
      fileAccess seek 8
      fileAccess writeLong print
    }
    fileAccess close()
  }

  def writeData(offsetInFileData: Long, bytes: Bytes, printOfBytes: Long): Unit =
    if (fileAccess.length() <= offsetInFileData + headerBytes)
      writeNewData(offsetInFileData, bytes, printOfBytes)
    else
      overwriteData(offsetInFileData, bytes, printOfBytes)

  def writeNewData(offsetInFileData: Long, bytes: Bytes, printOfBytes: Long): Unit = {
    fileAccess seek (offsetInFileData + headerBytes)
    fileAccess write (bytes.data, bytes.offset, bytes.length)
    print = print ^ printOfBytes
  }

  def overwriteData(offsetInFileData: Long, bytesToWrite: Bytes, printOfBytes: Long): Unit = {
    val bytesRead = read(offsetInFileData, bytesToWrite size)
    print = print ^ calcDataPrint(offsetInFileData, bytesRead)
    writeNewData(offsetInFileData, bytesToWrite, printOfBytes)
  }

  def read(offsetInFileData: Long, size: Size): Bytes =
    init(Bytes zero size) { bytes =>
      fileAccess seek (offsetInFileData + headerBytes)
      bytes fillFrom fileAccess
    }

  def recalculatePrint(): Unit = {
    val size = Size(fileAccess.length - headerBytes)
    val bytesRead = read(0, size)
    print = calcDataPrint(0, bytesRead)
  }
}
