package net.diet_rich.dedup.core.data.file

import java.io.{File, IOException, RandomAccessFile}

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

object DataFile {
  // Header bytes 1 - 8 : Data file number
  // Header bytes 9 - 16: Reserved for future data print extension
  val headerBytes = 16
}

class DataFile(dataFileNumber: Long, dataDir: File, readonly: Boolean) {
  import net.diet_rich.dedup.core.data.file.DataFile._

  private val file = dataDir / (f"$dataFileNumber%010X" grouped 2 mkString "/") // 00/00/00/00/00 (hex), max 10^12 files

  private val fileAccess: RandomAccessFile =
    if (readonly) openAndCheckHeader("r")
    else if (file exists()) openAndCheckHeader("rw")
    else init(new RandomAccessFile(file.withParentsMade(), "rw")){_ writeLong dataFileNumber}

  private def openAndCheckHeader(accessType: String): RandomAccessFile =
    init(new RandomAccessFile(file, accessType)) { fileAccess =>
      val fileNumberRead = fileAccess.readLong
      if (dataFileNumber != fileNumberRead) {
        fileAccess close()
        throw new IOException(s"Data file number read $fileNumberRead is not $dataFileNumber")
      }
    }

  def close() = synchronized { fileAccess close() }

  def writeData(offsetInFileData: Long, bytes: Bytes): Unit = synchronized {
    fileAccess seek (offsetInFileData + headerBytes)
    fileAccess write (bytes.data, bytes.offset, bytes.length)
  }

  def read(offsetInFileData: Long, size: Int): Bytes = init(Bytes zero size) { bytes => synchronized {
    fileAccess seek (offsetInFileData + headerBytes)
    fileAccess readFully (bytes.data, bytes.offset, bytes.length)
  } }
}
