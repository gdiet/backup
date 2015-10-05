package net.diet_rich.bytestore.file

import java.io.{IOException, RandomAccessFile, File}

import net.diet_rich.common._, io._

// Note: Synchronization and partitioning of data chunks is done in FileBackend.
private[file] object DataFile {
  //  0 -  7: (Long) Position of the first data byte in the data store
  //  8 - 15: (Long) Byte store name print
  // 16 - 23: (Long) File data print
  // 24 - 31: (Long) Reserved for future extension
  val headerBytes = 32

  // Performance optimized.
  private def calculateDataPrint(offsetInFileData: Long, bytes: Bytes): Long = internalDataPrint(offsetInFileData + 1, bytes)
  private def internalDataPrint(offsetInFileData: Long, bytes: Bytes): Long = {
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
    protected def fileAccess: Option[RandomAccessFile]

    final val file = dataDirectory / (f"$fileNumber%010X" grouped 2 mkString "/") // 00/00/00/00/00 (hex), max 10^12 files

    final protected def openAndCheckHeader(): RandomAccessFile =
      init(new RandomAccessFile(file, accessType)) { fileAccess =>
        def exceptionIfDifferent(expected: Long, what: String) = init(fileAccess.readLong) { read =>
          if (read != expected) { fileAccess close(); throw new IOException(s"Data file $fileNumber: $what read $read is not $expected") }
        }
        exceptionIfDifferent(startPosition, "Start position")
        exceptionIfDifferent(namePrint, "Name print")
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
    final def readRaw(offsetInFileData: Long, size: Int): Seq[Either[Int, Bytes]] =
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

  final class FileRead(val namePrint: Long, val dataDirectory: File, val fileNumber: Long, val startPosition: Long) extends FileCommonRead {
    val accessType = "r"
    protected val fileAccess: Option[RandomAccessFile] = if (file isFile()) Some(openAndCheckHeader()) else None
    override def close(): Unit = fileAccess foreach (_ close())
  }

  final class FileReadWrite(val namePrint: Long, val dataDirectory: File, val fileNumber: Long, val startPosition: Long) extends FileCommonRead {
    val accessType = "rw"
    private var dataPrint: Long = 0
    protected var fileAccess: Option[RandomAccessFile] = if (!file.isFile) None else Some (
      init(openAndCheckHeader()) { access => assert (access.getFilePointer == 16); dataPrint = access readLong() }
    )
    private def writeAccess: RandomAccessFile = fileAccess getOrElse {
      file.getParentFile mkdirs()
      init(new RandomAccessFile(file, accessType)) { access =>
        access writeLong startPosition
        access writeLong namePrint
        fileAccess = Some(access)
      }
    }
    override def close(): Unit = fileAccess foreach { access =>
      val onlyHeader = access.length() <= headerBytes
      access seek 16
      access writeLong dataPrint
      access close()
      if (onlyHeader) file.delete()
    }
    private def reverseDataPrint(access: RandomAccessFile, dataLength: Long, offsetInFile: Long, size: Int): Unit = {
      val effectiveSizeOverwritten = math.min(size, dataLength - offsetInFile).toInt
      access seek (offsetInFile + headerBytes)
      val data = init(new Array[Byte](effectiveSizeOverwritten))(access.readFully)
      dataPrint ^= calculateDataPrint(offsetInFile, Bytes(data))
    }
    def write(offsetInFile: Long, bytes: Bytes): Unit = {
      val access = writeAccess
      val dataLength = access.length() - headerBytes
      if (dataLength > offsetInFile) reverseDataPrint(access, dataLength, offsetInFile, bytes.length)
      access seek (offsetInFile + headerBytes)
      access write (bytes.data, bytes.offset, bytes.length)
      dataPrint ^= calculateDataPrint(offsetInFile, bytes)
    }
    def clear(offsetInFile: Long, size: Int): Unit = {
      val access = writeAccess
      val dataLength = access.length() - headerBytes
      if (dataLength > offsetInFile) { // otherwise, no need to change
        reverseDataPrint(access, dataLength, offsetInFile, size)
        if (dataLength <= offsetInFile + size) access setLength offsetInFile + headerBytes
        else {
          access seek (offsetInFile + headerBytes)
          access write new Array[Byte](size)
        }
      }
    }
  }
}
