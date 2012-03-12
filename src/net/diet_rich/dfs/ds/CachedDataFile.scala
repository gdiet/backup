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

  protected var all = new HeaderAndData
  
  def readData: Boolean = synchronized {
    ASSUME(all.allData.length == 0, "may not read if already data present")
    dirty = false
    if (file exists) {
      all = new HeaderAndData(dataFile readFullFile)
      fileOffset == all.readOffset && 
      dataLength == all.readLength && 
      dataPrint == all.readPrint
    } else {
      all = new HeaderAndData(Bytes(headerSize + dataLength))
      all writeOffset fileOffset
      all writeLength dataLength
      true
    }
  }

  private def dataPrint = Digester crcadler() writeAnd all.data getDigest

  def writeData: Unit = if (dirty) synchronized {
    ASSUME(all.allData.length > 0, "it seems the cached data file has not been properly initialized with readData")
    ASSUME(all.readLength == dataLength, "readLength " + all.readLength + " must be equal to dataLength " + dataLength)
    all writePrint dataPrint
    dataFile.writeAllData(all allData)
    dirty = false
  }

  def write(offset: Long, source: Bytes) : Option[Bytes] = synchronized {
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset < fileOffset + dataLength, "offset " + offset + " should be less than " + (fileOffset + dataLength))
    dirty = true
    val writeOffset = offset - fileOffset
    val writeLength = math.min(source length, fileOffset + dataLength - offset)
    all.data copyFrom (source keepFirst writeLength, writeOffset)
    if (writeLength < source.length) Some(source dropFirst (source.length - writeLength)) else None
  }

  def read(offset: Long, size: Long) : Bytes = synchronized {
    // TODO synchronization would be better here with read and write locks
    ASSUME(offset >= fileOffset, "offset " + offset + " should be at least " + fileOffset)
    ASSUME(offset + size <= fileOffset + dataLength, "offset+size " + (offset+size) + " should be less or equal to fileOffset+dataLength " + (fileOffset+dataLength))
    val readOffset = offset - fileOffset
    // TODO support partial reads up to end
    all.data dropFirst readOffset keepFirst size copyTo Bytes(size)
  }
  
}


object CachedDataFile {
  private val headerSize = 24
  
  // HEADER: offset / length / print
  class HeaderAndData(val allData: Bytes = Bytes(0)) {
    lazy val data = allData dropFirst headerSize
    lazy val readOffset = allData readLong
    lazy val readLength = allData dropFirst 8 readLong
    lazy val readPrint = allData dropFirst 16 readLong
    def writeOffset(offset: Long) : Unit = allData store offset
    def writeLength(length: Long) : Unit = allData dropFirst 8 store length
    def writePrint(print: Long) : Unit = allData dropFirst 16 store print
  }
}


