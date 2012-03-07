package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util._
import net.diet_rich.util.io.using
import net.diet_rich.util.data.{Bytes,Digester}
import CachedDataFile._

class CachedDataFile(val fileOffset: Long, val dataLength: Long, val file: File) {
  ASSUME(dataLength > 0, "dataLength " + dataLength + " must be positive.")
  ASSUME(file != null, "file must not be null")
  
  private val dataFile = new DataFile(headerSize + dataLength, file)

  private var dirty = false

  protected var data: Bytes = Bytes(0)

  protected def offset = data longFrom
  protected def offset_= (offset: Long) = data storeIn offset
  
  protected def length = data dropFirst 8 longFrom
  protected def length_= (length: Long) = data dropFirst 8 storeIn length
  
  protected def print = data dropFirst 16 longFrom
  protected def print_= (print: Long) = data dropFirst 16 storeIn print

  def readData: Boolean = synchronized {
    require(data.length == 0)
    if (file exists) {
      data = dataFile.readFullFile
      fileOffset == offset && dataLength == length && dataPrint == print
    } else {
      data = Bytes(headerSize + dataLength)
      offset = fileOffset
      length = dataLength
      true
    }
  }

  private def dataPrint = Digester crcadler() writeAnd (data dropFirst headerSize) getDigest

  def writeData: Unit = if (dirty) synchronized {
    require(data.length == headerSize + dataLength)
    print = dataPrint
    dataFile.writeAllData(data)
    dirty = false
  }

  def write(offset: Long, data: Bytes) : Option[Bytes] = synchronized {
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset < fileOffset + dataLength, "offset " + offset + " should be less than " + (fileOffset + dataLength))
    dirty = true
    val writeOffset = offset - fileOffset
    val writeLength = math.min(data length, fileOffset + dataLength - offset)
    this.data copyFrom (data keepFirst writeLength, writeOffset + headerSize)
    if (writeLength < data.length) Some(data dropFirst (data.length - writeLength)) else None
  }

  def read(offset: Long, size: Long) : Bytes = synchronized {
    // TODO synchronization would be better here with read and write locks
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset + size <= fileOffset + dataLength, "offset+size " + (offset+size) + " should be less or equal to fileOffset+dataLength " + (fileOffset+dataLength))
    val readOffset = offset - fileOffset
    data dropFirst (readOffset + headerSize) keepFirst size copyTo Bytes(size)
  }
  
}

object CachedDataFile {
  private val headerSize = 24
}
