package net.diet_rich.bytestore.file

import java.io.{IOException, RandomAccessFile, File}

import net.diet_rich.common._, io._

private[file] object DataFile {
  //  1 -  8: (Long) Position of the first data byte in the data store
  //  9 - 16: (Long) Reserved for future data print extension FIXME implement
  // 17 - 24: (Long) Reserved for future name print extension FIXME implement
  // 25 - 32: (Long) Reserved for future extension
  val headerBytes = 32

  trait Common extends AutoCloseable {
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
        init(new RandomAccessFile(file, accessType)) {_ writeLong startPosition}
      } else {
        init(new RandomAccessFile(file, accessType)) { fileAccess =>
          val startPositionRead = fileAccess.readLong
          if (startPosition != startPositionRead) {
            fileAccess close()
            throw new IOException(s"Data file $fileNumber: Start position read $startPositionRead is not $startPosition")
          }
        }
      }
  }

  trait CommonRead extends Common {
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

  trait CommonReadOnly { _: Common =>
    final val accessType = "r"
    final val fileAccess: Option[RandomAccessFile] = if (file isFile()) Some(openAndCheckHeader()) else None
  }

  final class Read(val dataDirectory: File, val fileNumber: Long, val startPosition: Long)
        extends CommonRead with CommonReadOnly

  final class ReadRaw(val dataDirectory: File, val fileNumber: Long, val startPosition: Long)
        extends Common with CommonReadOnly {
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

  final class ReadWrite(val dataDirectory: File, val fileNumber: Long, val startPosition: Long)
        extends Common with CommonRead {
    val accessType = "rw"
    // FIXME var fileAccess allows for lazy construction, so trying to read files does not create them
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
  }
}
