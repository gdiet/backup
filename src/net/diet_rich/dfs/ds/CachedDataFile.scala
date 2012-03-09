package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util._
import net.diet_rich.util.io.using
import net.diet_rich.util.data.{Bytes,Digester}
import CachedDataFile._

class CachedDataFile(val fileOffset: Long, val dataLength: Long, val file: File) {
  ASSUME(dataLength > 0, "dataLength " + dataLength + " must be positive.")
  ASSUME(file != null, "file must not be null")
  
  protected val dataFile = new DataFile(headerSize + dataLength, file)

  protected var dirty = true

  protected var allData: Bytes = Bytes(0)
  protected def data: Bytes = allData.dropFirst(headerSize)

  // HEADER: offset / length / print
  
  // TODO encapsulate and use val for read offset, length and print
  protected def offset = allData readLong
  protected def offset_= (offset: Long) = allData store offset
  
  protected def length = allData dropFirst 8 readLong
  protected def length_= (length: Long) = allData dropFirst 8 store length
  
  protected def print = allData dropFirst 16 readLong
  protected def print_= (print: Long) = allData dropFirst 16 store print

  def readData: Boolean = synchronized {
    ASSUME(allData.length == 0, "may not read alread data present")
    dirty = false
    if (file exists) {
      allData = dataFile.readFullFile
      fileOffset == offset && dataLength == length && dataPrint == print
    } else {
      allData = Bytes(headerSize + dataLength)
      offset = fileOffset
      length = dataLength
      true
    }
  }

  private def dataPrint = Digester crcadler() writeAnd data getDigest

  def writeData: Unit = if (dirty) synchronized {
    ASSUME(allData.length > 0, "it seems the cached data file has not been properly initialized with readData")
    ASSUME(length == dataLength, "length " + length + " must be equal to dataLength " + dataLength)
    print = dataPrint
    dataFile.writeAllData(allData)
    dirty = false
  }

  def write(offset: Long, source: Bytes) : Option[Bytes] = synchronized {
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset < fileOffset + dataLength, "offset " + offset + " should be less than " + (fileOffset + dataLength))
    dirty = true
    val writeOffset = offset - fileOffset
    val writeLength = math.min(source length, fileOffset + dataLength - offset)
    data copyFrom (source keepFirst writeLength, writeOffset)
    if (writeLength < source.length) Some(source dropFirst (source.length - writeLength)) else None
  }

  def read(offset: Long, size: Long) : Bytes = synchronized {
    // TODO synchronization would be better here with read and write locks
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset + size <= fileOffset + dataLength, "offset+size " + (offset+size) + " should be less or equal to fileOffset+dataLength " + (fileOffset+dataLength))
    val readOffset = offset - fileOffset
    // TODO support partial reads up to end
    data dropFirst readOffset keepFirst size copyTo Bytes(size)
  }
  
}

object CachedDataFile {
  private val headerSize = 24
}
