package net.diet_rich.bytestore.file

import java.io.{IOException, RandomAccessFile, File}

import net.diet_rich.common._, io._

// Note: Synchronization is done in FileBackend.
private[file] object DataFile {
  //  1 -  8: (Long) Position of the first data byte in the data store
  //  9 - 16: (Long) Byte store name print
  // 17 - 24: (Long) Reserved for future extension
  // 25 - 32: (Long) Reserved for future data print extension FIXME implement
  val headerBytes = 32

  // Performance optimized.
  def calcDataPrint(offsetInFileData: Long, bytes: Bytes): Long = internalCalcDataPrint(offsetInFileData + 1, bytes)
  private def internalCalcDataPrint(offsetInFileData: Long, bytes: Bytes): Long = {
    var print: Long = 0L
    for (n <- 0 until bytes.length) print ^= (n + offsetInFileData) * 5870203405204807807L * bytes.data(n + bytes.offset)
    print
  }

  trait Common extends AutoCloseable {
    val namePrint: Long
    val dataDirectory: File
    val fileNumber: Long
    val startPosition: Long
    val accessType: String
    val fileAccess: Option[RandomAccessFile]

    final def close(): Unit = fileAccess foreach (_ close())

    final val file = dataDirectory / (f"$fileNumber%010X" grouped 2 mkString "/") // 00/00/00/00/00 (hex), max 10^12 files

    final def openAndCheckHeader(): RandomAccessFile =
      if (!file.exists()) {
        file.getParentFile mkdirs()
        init(new RandomAccessFile(file, accessType)) { access =>
          access writeLong startPosition
          access writeLong namePrint
        }
      } else {
        init(new RandomAccessFile(file, accessType)) { fileAccess =>
          def exceptionIfDifferent(expected: Long, what: String) = init(fileAccess.readLong) { read =>
            if (read != expected) { fileAccess close(); throw new IOException(s"Data file $fileNumber: $what read $read is not $expected") }
          }
          exceptionIfDifferent(startPosition, "Start position")
          exceptionIfDifferent(namePrint, "Name print")
        }
      }
  }

  trait FileCommonRead extends Common {
    final def read(offsetInFileData: Long, size: Int): Bytes = init(Bytes zero size) { bytes =>
      fileAccess foreach { access =>
        val availableData = access.length() - headerBytes
        if (availableData > offsetInFileData) {
          val dataToRead = math.min(size, availableData - offsetInFileData).toInt // size is Int
          access seek (offsetInFileData + headerBytes)
          access readFully (bytes.data, bytes.offset, dataToRead)
        }
      }
    }
  }

  final class FileRead(val namePrint: Long, val dataDirectory: File, val fileNumber: Long, val startPosition: Long) extends FileCommonRead {
    val accessType = "r"
    val fileAccess: Option[RandomAccessFile] = if (file isFile()) Some(openAndCheckHeader()) else None
  }

  final class FileReadWriteRaw(val namePrint: Long, val dataDirectory: File, val fileNumber: Long, val startPosition: Long) extends FileCommonRead {
    val accessType = "rw"
    val fileAccess: Option[RandomAccessFile] = Some(openAndCheckHeader())
    def setLength(offsetInFile: Long): Unit = fileAccess foreach (_ setLength (offsetInFile + headerBytes))
    def write(offsetInFile: Long, bytes: Bytes): Unit = fileAccess foreach { access =>
      access seek (offsetInFile + headerBytes)
      access write (bytes.data, bytes.offset, bytes.length)
    }
    def clear(offsetInFile: Long, size: Long): Unit = fileAccess foreach { access =>
      if (access.length() <= headerBytes + offsetInFile + size) setLength(offsetInFile)
      else {
        access seek (offsetInFile + headerBytes)
        for (position <- 0L to size by FileBackend.dataChunkMaxSize) {
          val sizeToWrite = math.min(FileBackend.dataChunkMaxSize, size - position).toInt
          access write new Array[Byte](sizeToWrite)
        }
      }
    }
    def readRaw(offsetInFileData: Long, size: Int): Seq[Either[Int, Bytes]] =
      fileAccess.map { access =>
        val availableData = access.length() - headerBytes
        if (availableData <= offsetInFileData) Seq(Left(size))
        else {
          val dataToRead = math.min(size, availableData - offsetInFileData).toInt // size is Int
          access seek (offsetInFileData + headerBytes)
          val bytes = Bytes.zero(dataToRead)
          access readFully (bytes.data, bytes.offset, dataToRead)
          if (dataToRead < size) Seq(Right(bytes), Left(size - dataToRead)) else Seq(Right(bytes))
        }
      } getOrElse Seq(Left(size))
  }
}
