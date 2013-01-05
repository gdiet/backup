// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.File
import java.io.RandomAccessFile
import net.diet_rich.dedup.database._
import net.diet_rich.util.ByteArrayUtil._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class DataFileException(message: String, val data: Array[Byte]) extends java.io.IOException(message)

class DataFile(dataLength: Size, file: File) { import DataFile._
  assume (dataLength > Size(0), "data length of file %s must be positive but is %s" format (file, dataLength))
  assume (dataLength < Size(Int.MaxValue), "data length of file %s must less than MaxInt but is %s" format (file, dataLength))
  assume (!file.exists || file.isFile, "data file %s must be a regular file if it exists" format file)

  val bytes = new Array[Byte](dataLength.value toInt)
  if (file.exists()) {
    readAndPadZeros(file, bytes)
    val print = CrcAdler8192.calculatePrint(bytes, headerSize, bytes.length - headerSize)
    if (print != Print(readLong(bytes, 0)))
      throw new DataFileException("CRC of datafile does not match", bytes)
  }
  
  def write: Unit = {
    val print = CrcAdler8192.calculatePrint(bytes, headerSize, bytes.length - headerSize)
    writeLong(bytes, 0, print.value)
    using(new RandomAccessFile(file, "rw")){ _ write (bytes) }
  }
}

object DataFile {
  val headerSize = 8
  
  def readAndPadZeros(file: File, bytes: Array[Byte], offsetInFile: Position = Position(0)): Unit = {
    assume (file.isFile, "data file %s must be an existing regular file" format file)
    assume (offsetInFile > Position(-1), "offset in datafile %s must be positive but is %s" format (file, offsetInFile))
    
    using(new RandomAccessFile(file, "r")){ reader =>
      reader.seek(offsetInFile.value)
      fillFrom(reader, bytes, 0, bytes.length)
    }
  }
}
