// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import net.diet_rich.util.io.Streams.usingIt

/**
 * always writes files of DSSettings.dataFileChunkSize + 8 bytes length (zero-padded where necessary)
 * the last 8 bytes are the DSSettings.newDataFileChecksum Long checksum
 */
private[datastore]
trait DSWFileTrait
extends net.diet_rich.util.logging.Logged {
  
  // members required in implementing class
  protected val settings: DSSettings
  protected val file: java.io.File
  protected val source: Option[java.io.File]
  /** method parameters: data bytes, data byte offset, data length, file position */
  protected val storeMethod: (Array[Byte], Int, Int, Int) => Unit
  
  // import companion members
  import DSWFileTrait._
  
  /** the data array to store the data in temporarily */
  protected val dataArray = new Array[Byte](settings.dataFileChunkSize)

  private def dataChecksum : Long = {
      val checksum = settings.newDataFileChecksum
      checksum.update(dataArray, 0, dataArray.length)
      return checksum.getValue
  }
  
  // initialize from source file if source is defined
  source foreach { sourceFile =>
    usingIt(new java.io.RandomAccessFile(sourceFile,  "r")){raFile => 
      // check source size
      val size = raFile.length
      if (size != settings.dataFileChunkSize + 8)
        throwError(new DataFileSizeException("data file size error"), sourceFile, size)
      // copy data from source to data array and check checksum
      raFile.readFully(dataArray)
      if (dataChecksum != raFile.readLong)
        throwError(new DataFileChecksumException("data file checksum error"), sourceFile)
    }
  }

  private var _isOpen = true
  protected def isOpen = _isOpen // getter
  private def isOpen_= (value: Boolean) : Unit = _isOpen = value // setter
  
  /** store the bytes in the writer cache. thread safe synchronized. */
  final def store(bytes: Array[Byte], offset: Int, length: Int, position: Int) : Unit = {
    assume(length >= 0)
    assume(position >= 0)
    assume(position <= settings.dataFileChunkSize - length)
    assume(offset <= bytes.length - length)
    synchronized { 
      require(isOpen)
      storeMethod(bytes, offset, length, position)
    }
  }

  /** write cache to file. not final for testability only. */
  def close() : Unit = {
    synchronized { 
      require(isOpen)
      isOpen = false
      usingIt(new java.io.RandomAccessFile(file,  "rw")){raFile => 
        raFile write dataArray
        raFile writeLong dataChecksum
      }
    }
  }
  
}

object DSWFileTrait {
  class DataFileWriterException(message: String) extends Exception(message)
  class DataFileSizeException(message: String) extends DataFileWriterException(message)
  class DataFileChecksumException(message: String) extends DataFileWriterException(message)
}