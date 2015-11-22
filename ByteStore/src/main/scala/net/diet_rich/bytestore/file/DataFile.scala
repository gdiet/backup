package net.diet_rich.bytestore.file

import java.io.{IOException, RandomAccessFile, File}

import net.diet_rich.common._, io._, vals._

// Note: Synchronization and partitioning of data chunks is done in FileBackend.
private[file] object DataFile {
  //  0 -  7: (Long) Position of the first data byte in the data store
  //  8 - 15: (Long) Byte store name print
  // 16 - 23: (Long) File data print
  // 24 - 31: (Long) Reserved for future extension
  val headerBytes = 32  // TODO design useful values class for DataFile

  // Performance optimized.
  private def calculateDataPrint(offsetInFileData: Long, bytes: Bytes): Long = internalDataPrint(offsetInFileData + 1, bytes)
  private def internalDataPrint(offsetInFileData: Long, bytes: Bytes): Long = {
    var print: Long = 0L
    for (n <- 0 until bytes.length) print ^= (n + offsetInFileData) * 5870203405204807807L * bytes.data(n + bytes.offset)
    print
  }

  trait Common extends AutoCloseable with Logging {
    val namePrint: Print
    val dataDirectory: File
    val fileNumber: FileNumber
    val startPosition: Position
    val accessType: String
    protected def fileAccess: Option[RandomAccessFile]

    final val file = dataDirectory / (f"${fileNumber.value}%010X" grouped 2 mkString "/") // 00/00/00/00/00 (hex), max 10^12 files
    log debug s"accessing start position $startPosition in file number $fileNumber: data file $file"

    final protected def openAndCheckHeader(): RandomAccessFile =
      init(new RandomAccessFile(file, accessType)) { fileAccess =>
        def exceptionIfDifferent(expected: Long, what: String) = init(fileAccess.readLong) { read =>
          if (read != expected) { fileAccess close(); throw new IOException(s"Data file $fileNumber: $what read $read is not $expected") }
        }
        exceptionIfDifferent(startPosition.value, "Start position")
        exceptionIfDifferent(namePrint.value, "Name print")
      }
  }

  trait FileCommonRead extends Common {
    final def read(offsetInFileData: Offset, size: IntSize): Bytes = init(Bytes zero size.value) { bytes =>
      fileAccess foreach { access =>
        def offsetInFile = offsetInFileData.value
        val availableData = access.length() - headerBytes
        if (availableData > offsetInFile) {
          val dataToRead = min(size, availableData - offsetInFile)
          access seek (offsetInFile + headerBytes)
          access readFully (bytes.data, bytes.offset, dataToRead)
        }
      }
    }
    final def readRaw(offsetInFileData: Offset, size: IntSize): Seq[Either[Int, Bytes]] =
      fileAccess.map { access =>
        def offsetInFile = offsetInFileData.value
        val availableData = access.length() - headerBytes
        if (availableData <= offsetInFile) Seq(Left(size.value))
        else {
          val dataToRead = min(size, availableData - offsetInFile)
          access seek (offsetInFile + headerBytes)
          val bytes = Bytes.zero(dataToRead)
          access readFully (bytes.data, bytes.offset, dataToRead)
          if (dataToRead < size.value) Seq(Right(bytes), Left(size.value - dataToRead)) else Seq(Right(bytes))
        }
      } getOrElse Seq(Left(size.value))
  }

  final class FileRead(val namePrint: Print, val dataDirectory: File, val fileNumber: FileNumber, val startPosition: Position) extends FileCommonRead {
    val accessType = "r"
    protected val fileAccess: Option[RandomAccessFile] = if (file isFile()) Some(openAndCheckHeader()) else None
    override def close(): Unit = fileAccess foreach (_ close())
  }

  final class FileReadWrite(val namePrint: Print, val dataDirectory: File, val fileNumber: FileNumber, val startPosition: Position) extends FileCommonRead {
    val accessType = "rw"
    private var dataPrint: Long = 0
    protected var fileAccess: Option[RandomAccessFile] = if (!file.isFile) None else Some (
      init(openAndCheckHeader()) { access => assert (access.getFilePointer == 16); dataPrint = access readLong() }
    )
    private def writeAccess: RandomAccessFile = fileAccess getOrElse {
      file.getParentFile mkdirs()
      init(new RandomAccessFile(file, accessType)) { access =>
        access writeLong startPosition.value
        access writeLong namePrint.value
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
    def write(offsetInFileData: Offset, bytes: Bytes): Unit = {
      def offsetInFile = offsetInFileData.value
      val access = writeAccess
      val dataLength = access.length() - headerBytes
      if (dataLength > offsetInFile) reverseDataPrint(access, dataLength, offsetInFile, bytes.length)
      access seek (offsetInFile + headerBytes)
      access write (bytes.data, bytes.offset, bytes.length)
      dataPrint ^= calculateDataPrint(offsetInFile, bytes)
    }
    def clear(offsetInFileData: Offset, size: IntSize): Unit = {
      def offsetInFile = offsetInFileData.value
      val access = writeAccess
      val dataLength = access.length() - headerBytes
      if (dataLength > offsetInFile) { // otherwise, no need to change
        reverseDataPrint(access, dataLength, offsetInFile, size.value)
        if (dataLength <= offsetInFile + size.value) access setLength offsetInFile + headerBytes
        else {
          access seek (offsetInFile + headerBytes)
          access write new Array[Byte](size.value)
        }
      }
    }
  }
}
