package net.diet_rich.dfs.sds

import java.io.File
import net.diet_rich.util.ASSUME
import net.diet_rich.util.Bytes
import net.diet_rich.util.PrintDigester.crcAdler
import CachedDataFile._

class CachedDataFile(val fileOffset: Long, val dataLength: Long, val file: File) {
  ASSUME(fileOffset % dataLength == 0, "file offset %s must divisible by dataLength %s" format (fileOffset, dataLength))
  
  val isNewFile = !file.exists
  
  protected val fullSize = dataLength + headerSize
  protected val dataFile = new DataFile(fullSize, file)
  protected val fullData = if (isNewFile) Bytes(fullSize) else dataFile readAndPadZeros()
  protected val contents = fullData dropFirst headerSize

  val isValidRead = isNewFile || {
    val fileOffsetOk = fileOffset == fullData.readLong(0)
    val dataLengthOk = dataLength == fullData.readLong(8)
    val dataPrintOk = dataPrint(contents) == fullData.readLong(16)
    fileOffsetOk && dataLengthOk && dataPrintOk
  }
  
  protected var dirty = false

  def flush: Unit = if (dirty) {
    fullData writeLong(0, fileOffset) writeLong(8, dataLength) writeLong(16, dataPrint(contents))
    dataFile.writeFully(fullData)
    dirty = false
  }

  def write(offset: Long, data: Bytes) : Option[Bytes] = {
    ASSUME(offset >= fileOffset, "offset %s should be at least fileOffset %s" format (offset, fileOffset))
    ASSUME(offset < fileOffset + dataLength, "offset %s should be less than fileOffset + dataLength %s" format (offset, fileOffset + dataLength))
    val writeOffset = offset - fileOffset
    val writeLength = math.min(data size, dataLength - writeOffset)
    dirty = true
    contents copyFrom (data setSize writeLength, writeOffset)
    if (writeLength < data.size) Some(data dropFirst (data.size - writeLength)) else None
  }
  
  def dataView(offset: Long, size: Long) : Bytes = {
    ASSUME(offset >= fileOffset, "offset %s should be at least fileOffset %s" format (offset, fileOffset))
    ASSUME(offset < fileOffset + dataLength, "offset %s should be less than fileOffset + dataLength %s" format (offset, fileOffset + dataLength))
    val viewOffset = offset - fileOffset
    val viewLength = math.min(size, dataLength - viewOffset)
    contents dropFirst viewOffset setSize viewLength
  }
  
}


object CachedDataFile {
  private val headerSize = 24
  def dataPrint(contents: Bytes) = crcAdler write contents digest
}
