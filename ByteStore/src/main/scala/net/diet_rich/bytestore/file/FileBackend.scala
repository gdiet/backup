package net.diet_rich.bytestore.file

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

import net.diet_rich.bytestore._
import net.diet_rich.common._, io._

object FileBackend { val version = "3.0"

  private val configFileName = "config.txt"
  private val (nameKey, versionKey, blocksizeKey) = ("name", "version", "blocksize")
  private val maxNumberOfOpenFiles = 64
  private[file] val dataChunkMaxSize: Int = 65536 // Int to avoid problems with byte array size

  def initializeDirectory(dataDirectory: File, name: String, blocksize: Long): Unit = {
    require(!dataDirectory.exists(), s"byte store data directory already exists: $dataDirectory")
    require(dataDirectory mkdir(), s"can't create byte store data directory $dataDirectory")
    require(blocksize > 0, "block size must be positive")
    writeSettingsFile(dataDirectory / configFileName, Map(
      versionKey -> version,
      nameKey -> name,
      blocksizeKey -> s"$blocksize"
    ))
  }

  def read(dataDirectory: File, name: String): ByteStoreRead = new FileBackendRead(dataDirectory, name)
  def readWrite(dataDirectory: File, name: String): ByteStore = new FileBackendReadWrite(dataDirectory, name)
  // FIXME raw is needed with write to allow for backend-with-underlying-backend
  def readRaw(dataDirectory: File, name: String): ByteStoreReadRaw = new FileBackendReadRaw(dataDirectory, name)

  private trait Common[DataFileType <: AutoCloseable] {
    val dataDirectory: File
    val name: String
    def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): DataFileType

    final val blocksize: Long = {
      val settings = readSettingsFile(dataDirectory / configFileName)
      require(settings(versionKey) == version, s"Version mismatch in file byte store: Actual ${settings(versionKey)}, required $version")
      require(settings(nameKey) == name, s"Name mismatch in file byte store: Actual ${settings(nameKey)}, expected $name")
      settings(blocksizeKey).toLong
    }

    /** @return data file number / offset in file / number of bytes */
    final def blockStream(from: Long, size: Long, chunkMaxSize: Long): Stream[(Long, Long, Long)] =
      if (size <= 0L) { require (size == 0L, s"Tried to use a negative number of bytes: $size"); Stream.empty }
      else {
        val dataFileNumber = from / blocksize
        val offsetInFile = from % blocksize
        val bytesToRead = math.min(chunkMaxSize, math.min(blocksize - offsetInFile, size))
        (dataFileNumber, offsetInFile, bytesToRead) #:: blockStream(from + bytesToRead, size - bytesToRead, chunkMaxSize)
      }

    object dataFiles {
      private val dataFiles = mutable.LinkedHashMap.empty[Long, (DataFileType, ReentrantLock)] // traversal in insertion order

      def apply[T](fileNumber: Long)(f: DataFileType => T): T = {
        val (file, lock) = dataFiles synchronized {
          val (file, lock) = dataFiles.remove(fileNumber)
                .getOrElse((dataFile(dataDirectory, fileNumber, fileNumber * blocksize), new ReentrantLock()))
          if (dataFiles.size >= maxNumberOfOpenFiles)
            (dataFiles remove dataFiles.keys.head) foreach { case (oldDataFile, _) => oldDataFile close()}
          dataFiles += (fileNumber -> (file, lock))
          lock lock() // important: acquire the data file lock *before* leaving the synchronized block
          (file, lock)
        }
        try f(file) finally lock unlock() // important: execute f *after* leaving the synchronized block
      }

      def close(): Unit = dataFiles synchronized {
        dataFiles.values foreach { case (dataFile, lock) => // first-to-last: close the oldest datafiles first
          lock.lock()
          try dataFile close() finally lock unlock()
        }
        dataFiles clear()
      }
    }

    final def nextBlockStart(position: Long): Long = position % blocksize match {
      case 0 => position
      case n => position + blocksize - n
    }
    final def close(): Unit = dataFiles close()
  }

  private trait CommonRead[DataFileType <: DataFile.CommonRead] extends Common[DataFileType] {
    final def read(from: Long, to: Long): Iterator[Bytes] =
      blockStream(from, to - from, dataChunkMaxSize).iterator map {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ read(offset, length.toInt)) // toInt: dataChunkMaxSize is Int
      }
  }

  private final class FileBackendRead(val dataDirectory: File, val name: String) extends CommonRead[DataFile.Read] with ByteStoreRead {
    override def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): DataFile.Read =
      new DataFile.Read(dataDirectory, startPosition, fileNumber)
  }

  private final class FileBackendReadRaw(val dataDirectory: File, val name: String) extends Common[DataFile.ReadRaw] with ByteStoreReadRaw {
    override def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): DataFile.ReadRaw =
      new DataFile.ReadRaw(dataDirectory, startPosition, fileNumber)
    override def readRaw(from: Long, to: Long): Iterator[Either[Int, Bytes]] =
      blockStream(from, to - from, dataChunkMaxSize).iterator flatMap {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ readRaw(offset, length.toInt)) // toInt: dataChunkMaxSize is Int
      }
  }

  private final class FileBackendReadWrite(val dataDirectory: File, val name: String) extends CommonRead[DataFile.ReadWrite] with ByteStore {
    override def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): DataFile.ReadWrite =
      new DataFile.ReadWrite(dataDirectory, startPosition, fileNumber)
    @annotation.tailrec
    override def write(data: Bytes, at: Long): Unit = {
      val offsetInFile = at % blocksize
      val bytesToWriteInDataFile = math.min(blocksize - offsetInFile, data.length).toInt // Int because data.length is Int
      dataFiles(at / blocksize)(_ write(offsetInFile, data withLength bytesToWriteInDataFile))
      if (data.length > bytesToWriteInDataFile) write(data withOffset bytesToWriteInDataFile, at + bytesToWriteInDataFile)
    }
    override def clear(from: Long, to: Long): Unit =
      blockStream(from, to - from, blocksize) foreach {
        case (dataFileNumber, offset, length) => dataFiles(dataFileNumber) { dataFile =>
          if (offset + length == blocksize) dataFile setLength offset else dataFile clear(offset, length)
        }
      }
  }
}
