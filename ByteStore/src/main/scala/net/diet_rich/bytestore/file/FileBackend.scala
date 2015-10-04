package net.diet_rich.bytestore.file

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

import net.diet_rich.bytestore._
import net.diet_rich.common._, io._

import DataFile._

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

  def read(dataDirectory: File, name: String): ByteStoreRead = readRaw(dataDirectory, name)
  def readRaw(dataDirectory: File, name: String): ByteStoreRead with ByteStoreReadRaw = new FileBackendRead(dataDirectory, name)
  def readWrite(dataDirectory: File, name: String): ByteStore = readWriteRaw(dataDirectory, name)
  def readWriteRaw(dataDirectory: File, name: String): ByteStore with ByteStoreReadRaw = new FileBackendReadWriteRaw(dataDirectory, name)

  private trait Common[DataFileType <: AutoCloseable] {
    val dataDirectory: File
    val name: String
    def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): DataFileType

    final val namePrint: Long = printOf(name)

    final val blocksize: Long = {
      val settings = readSettingsFile(dataDirectory / configFileName)
      require(settings(versionKey) == version, s"Version mismatch in file byte store: Actual ${settings(versionKey)}, required $version")
      require(settings(nameKey) == name, s"Name mismatch in file byte store: Actual ${settings(nameKey)}, expected $name")
      settings(blocksizeKey).toLong
    }

    /** @return data file number / offset in file / number of bytes */
    final def blockStream(from: Long, size: Long): Stream[(Long, Long, Int)] =
      if (size <= 0L) { require (size == 0L, s"Tried to use a negative number of bytes: $size"); Stream.empty }
      else {
        val dataFileNumber = from / blocksize
        val offsetInFile = from % blocksize
        val bytesToRead = math.min(dataChunkMaxSize, math.min(blocksize - offsetInFile, size)).toInt
        (dataFileNumber, offsetInFile, bytesToRead) #:: blockStream(from + bytesToRead, size - bytesToRead)
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
          (file, init(lock)(_ lock())) // important: acquire the data file lock *before* leaving the synchronized block
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

  private trait CommonRead[DataFileType <: FileCommonRead] extends Common[DataFileType] with ByteStoreReadRaw {
    final def read(from: Long, to: Long): Iterator[Bytes] =
      blockStream(from, to - from).iterator map {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ read(offset, length.toInt)) // toInt: dataChunkMaxSize is Int
      }
    final def readRaw(from: Long, to: Long): Iterator[Either[Int, Bytes]] =
      blockStream(from, to - from).iterator flatMap {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ readRaw(offset, length.toInt)) // toInt: dataChunkMaxSize is Int
      }
  }

  private final class FileBackendRead(val dataDirectory: File, val name: String) extends CommonRead[FileRead]
  with ByteStoreRead {
    override def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): FileRead =
      new FileRead(namePrint, dataDirectory, startPosition, fileNumber)
  }

  private final class FileBackendReadWriteRaw(val dataDirectory: File, val name: String) extends CommonRead[FileReadWrite]
  with ByteStore {
    // FIXME clean closed flag and lock file
    override def dataFile(dataDirectory: File, fileNumber: Long, startPosition: Long): FileReadWrite =
      new FileReadWrite(namePrint, dataDirectory, startPosition, fileNumber)
    // Note: In the current implementation of DataFile, up to data.length bytes of RAM may be additionally allocated while writing
    @annotation.tailrec
    override def write(data: Bytes, at: Long): Unit = {
      val offsetInFile = at % blocksize
      val bytesToWriteInDataFile = math.min(blocksize - offsetInFile, data.length).toInt // Int because data.length is Int
      dataFiles(at / blocksize)(_ write(offsetInFile, data withLength bytesToWriteInDataFile))
      if (data.length > bytesToWriteInDataFile) write(data withOffset bytesToWriteInDataFile, at + bytesToWriteInDataFile)
    }
    override def clear(from: Long, to: Long): Unit =
      blockStream(from, to - from).reverse foreach { // reverse makes clear more efficient
        case (dataFileNumber, offset, length) => dataFiles(dataFileNumber) { _ clear (offset, length) }
      }
  }
}
