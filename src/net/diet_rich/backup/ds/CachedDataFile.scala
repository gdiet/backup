// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.ds

import java.io.File
import net.diet_rich.util.{ByteArrayUtil,ImmutableBytes}
import net.diet_rich.backup.CrcAdler
import CachedDataFile._

abstract class CachedDataFile(val storeOffset: Long, val dataLength: Int, val file: File) {
  assume(storeOffset % dataLength == 0, "file offset in data store %s must divisible by file data length %s" format (storeOffset, dataLength))
  assume(Int.MaxValue - headerSize >= dataLength, "data length %s + header size %s must not be greater than MaxInt" format (dataLength, headerSize))

  protected val fullSize = dataLength + headerSize
  protected val dataFile = new DataFile(fullSize, file)
  protected val isNewFile = !file.exists
  
  protected var dirty = false
  protected var (payload, bytes) = if (isNewFile) (headerSize, new Array[Byte](fullSize)) else dataFile readAndPadZeros()

  val isValidInitialized = isNewFile || (
    (storeOffset == ByteArrayUtil.readLong(bytes, 0))
    && (dataLength == ByteArrayUtil.readLong(bytes, 8))
    && (dataPrint == ByteArrayUtil.readLong(bytes, 16))
  )
  
  def flush: Unit = if (dirty) {
    ByteArrayUtil.writeLongs(bytes, 0, storeOffset, dataLength, dataPrint)
    dataFile.writeFully(bytes)
    dirty = false
  }
  
  def dataPrint =
    CrcAdler.print(bytes, headerSize, payload - headerSize)
  
//  val isNewFile = !file.exists
//  
//  protected val fullSize = dataLength + headerSize
//  protected val dataFile = new DataFile(fullSize, file)
//  protected val fullData = if (isNewFile) Bytes(fullSize) else dataFile readAndPadZeros()
//  protected val contents = fullData dropFirst headerSize
//
//  val isValidRead = isNewFile || {
//    val fileOffsetOk = fileOffset == fullData.readLong(0)
//    val dataLengthOk = dataLength == fullData.readLong(8)
//    val dataPrintOk = dataPrint(contents) == fullData.readLong(16)
//    fileOffsetOk && dataLengthOk && dataPrintOk
//  }
//  
//  def write(offset: Long, data: Bytes) : Option[Bytes] = {
//    ASSUME(offset >= fileOffset, "offset %s should be at least fileOffset %s" format (offset, fileOffset))
//    ASSUME(offset < fileOffset + dataLength, "offset %s should be less than fileOffset + dataLength %s" format (offset, fileOffset + dataLength))
//    val writeOffset = offset - fileOffset
//    val writeLength = math.min(data size, dataLength - writeOffset)
//    dirty = true
//    contents copyFrom (data setSize writeLength, writeOffset)
//    if (writeLength < data.size) Some(data dropFirst (data.size - writeLength)) else None
//  }
//  
//  def dataView(offset: Long, size: Long) : Bytes = {
//    ASSUME(offset >= fileOffset, "offset %s should be at least fileOffset %s" format (offset, fileOffset))
//    ASSUME(offset < fileOffset + dataLength, "offset %s should be less than fileOffset + dataLength %s" format (offset, fileOffset + dataLength))
//    val viewOffset = offset - fileOffset
//    val viewLength = math.min(size, dataLength - viewOffset)
//    contents dropFirst viewOffset setSize viewLength
//  }
//  
}


object CachedDataFile {
  /** file header is:
   *  fileOffset: Long
   *  dataLength: Long
   *  dataPrint: Long
   */
  private val headerSize = 24
}
