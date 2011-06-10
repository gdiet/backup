// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import net.diet_rich.util.io.Streams.{readBytes,usingIt}

/**
 * always writes files of DSSettings.dataFileChunkSize + 8 bytes length (0-padded if necessary)
 * the last 8 bytes are the DSSettings.newDataFileChecksum Long checksum
 */
private[datastore]
class DSDataFileWriter(settings: DSSettings, file: java.io.File, ecWriter: DSECFileWriter, source: Option[java.io.File] = None)
extends net.diet_rich.util.logging.Logged {
  
  // import companion members
  import DSDataFileWriter._
  
  /** the data array to store the data in temporarily */
  private val dataArray = new Array[Byte](settings.dataFileChunkSize)
  
  // if source is defined
  source foreach { sourceFile =>
    usingIt(new java.io.RandomAccessFile(sourceFile,  "r")){raFile => 
      // check source size
      val size = raFile.length
      if (size != settings.dataFileChunkSize + 8)
        throwError(new DataFileSizeException, "data file size error", file, size)
      // copy data from source to data array and check checksum
      val read = readBytes(raFile, dataArray, size - 8)
      if (read != size - 8) throwError(new DataFileSizeException, "data file size error", file, read)
      val checksum = settings.newDataFileChecksum
      checksum.update(dataArray, 0, read)
      if (checksum.getValue != raFile.readLong) throwError(new DataFileChecksumException, "data file checksum error", file)
    }
  }

  /**
   * store bytes at the given position, updating the error correction data accordingly
   */
  def store(bytes: Array[Byte], offset: Int, length: Int, position: Int) : Unit = {
    assume(length >= 0)
    assume(position >= 0)
    assume(position <= settings.dataFileChunkSize - length)
    synchronized {
      ecWriter.write(dataArray, position, length, position)
      for (n <- 0 until length) {
        dataArray(position + n) = dataArray(position + n) ^ bytes(offset + n) toByte
      }
      ecWriter.write(dataArray, position, length, position)
    }
  }
  
  // FIXME def close : Unit with write-to-file including checksum
  
}

object DSDataFileWriter {
  class DataFileWriterException extends Exception
  class DataFileSizeException extends DataFileWriterException
  class DataFileChecksumException extends DataFileWriterException
}