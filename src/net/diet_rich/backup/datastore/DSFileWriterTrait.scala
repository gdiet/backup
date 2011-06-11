// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import net.diet_rich.util.io.Streams.{readBytes,usingIt}

/**
 * always writes files of DSSettings.dataFileChunkSize + 8 bytes length (0-padded if necessary)
 * the last 8 bytes are the DSSettings.newDataFileChecksum Long checksum
 */
private[datastore]
trait DSFileWriterTrait
extends net.diet_rich.util.logging.Logged {
  
  // members required from implementing class
  protected val settings: DSSettings
  protected val file: java.io.File
  protected val source: Option[java.io.File]
  
  // import companion members
  import DSFileWriterTrait._
  
  /** the data array to store the data in temporarily */
  protected val dataArray = new Array[Byte](settings.dataFileChunkSize)

  // initialize from source file if source is defined
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

  def storeChecks(bytes: Array[Byte], offset: Int, length: Int, position: Int) : Unit = {
    assume(length >= 0)
    assume(position >= 0)
    assume(position <= settings.dataFileChunkSize - length)
    assume(offset <= bytes.length - length)
  }

  
  // FIXME def close : Unit with write-to-file including checksum
  
}

object DSFileWriterTrait {
  class DataFileWriterException extends Exception
  class DataFileSizeException extends DataFileWriterException
  class DataFileChecksumException extends DataFileWriterException
}