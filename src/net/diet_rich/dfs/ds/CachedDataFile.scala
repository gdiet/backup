package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util.io.using
import net.diet_rich.util.data.{Bytes,Digester}

class CachedDataFile(val fileOffset: Long, val dataLength: Long, val file: File) {
  import CachedDataFile._

  private val dataFile = new DataFile(headerSize + dataLength, file)
  
  private var data: Bytes = Bytes(0)
  private var dirty = false

  private def offset = data longFrom
  private def offset_= (offset: Long) = data storeIn offset
  
  private def length = data dropFirst 8 longFrom
  private def length_= (length: Long) = data dropFirst 8 storeIn length
  
  private def print = data dropFirst 16 longFrom
  private def print_= (print: Long) = data dropFirst 16 storeIn print

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

  def write(offset: Long, data: Bytes) : Option[Bytes] = {
    require(offset >= fileOffset)
    require(offset < fileOffset + dataLength)
    val writeOffset = offset - fileOffset
    val writeLength = math.min(data length, fileOffset + dataLength - offset)
    this.data copyFrom (data keepFirst writeLength, writeOffset + headerSize)
    if (writeLength < data.length) Some(data dropFirst writeLength) else None
  }
  
}

object CachedDataFile {
  private val headerSize = 24
}
