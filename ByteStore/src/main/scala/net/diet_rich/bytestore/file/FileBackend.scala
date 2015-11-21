package net.diet_rich.bytestore.file

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

import net.diet_rich.bytestore._
import net.diet_rich.common._

object FileBackend extends DirWithConfigHelper { import DataFile._
  override val (objectName, version) = ("file byte store", "3.0")
  private val (blocksizeKey, maxNumberOfOpenFiles) = ("block size", 64)
  private val dataChunkMaxSize = IntSize(65536) // Int to avoid problems with byte array size

  def initialize(directory: File, name: String, blocksize: Long): Unit = {
    require(blocksize > 0, "Block size must be positive")
    initialize(directory, name, Map(blocksizeKey -> s"$blocksize"))
  }

  def read(dataDirectory: File, name: String): ByteStoreRead = readRaw(dataDirectory, name)
  def readRaw(dataDirectory: File, name: String): ByteStoreRead with ByteStoreReadRaw = new FileBackendRead(dataDirectory, name)
  def readWrite(dataDirectory: File, name: String): ByteStore = readWriteRaw(dataDirectory, name)
  def readWriteRaw(dataDirectory: File, name: String): ByteStore with ByteStoreReadRaw = new FileBackendReadWriteRaw(dataDirectory, name)

  def nextBlockStart(blocksize: BlockSize, position: Position): Position = position % blocksize match {
    case Offset(0) => position
    case offset => position + blocksize - offset
  }


  private trait Common[DataFileType <: FileCommonRead] extends ByteStoreRead with ByteStoreReadRaw {
    val directory: File
    val name: String
    def dataFile(dataDirectory: File, fileNumber: FileNumber, startPosition: Position): DataFileType

    final val namePrint: Print = printOf(name)
    final val blocksize: BlockSize = BlockSize(settingsChecked(directory, name)(blocksizeKey).toLong)

    /** @return data file number / offset in file / number of bytes */
    final def blockStream(from: Position, size: Size): Stream[(FileNumber, Offset, IntSize)] =
      if (size == Size(0)) Stream.empty else {
        size.requirePositive()
        val dataFileNumber = from / blocksize
        val offsetInFile = from % blocksize
        val bytesToRead = min(dataChunkMaxSize, blocksize - offsetInFile, size)
        (dataFileNumber, offsetInFile, bytesToRead) #:: blockStream(from + bytesToRead, size - bytesToRead)
      }

    object dataFiles {
      private val dataFiles = mutable.LinkedHashMap.empty[FileNumber, (DataFileType, ReentrantLock)] // traversal in insertion order

      def apply[T](fileNumber: FileNumber)(f: DataFileType => T): T = {
        val (file, lock) = dataFiles synchronized {
          val (file, lock) = dataFiles.remove(fileNumber)
                .getOrElse((dataFile(directory, fileNumber, fileNumber * blocksize), new ReentrantLock()))
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

    final override def read(from: Long, to: Long): Iterator[Bytes] =
      blockStream(Position(from), Size(to - from)).iterator map {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ read(offset, length))
      }
    final override def readRaw(from: Long, to: Long): Iterator[Either[Int, Bytes]] =
      blockStream(Position(from), Size(to - from)).iterator flatMap {
        case (dataFileNumber, offset, length) =>
          dataFiles(dataFileNumber)(_ readRaw(offset, length))
      }
  }


  private final class FileBackendRead(val directory: File, val name: String) extends Common[FileRead]
  with ByteStoreRead {
    override def dataFile(dataDirectory: File, fileNumber: FileNumber, startPosition: Position): FileRead =
      new FileRead(namePrint, dataDirectory, fileNumber, startPosition)
    override def close(): Unit = dataFiles close()
  }

  private final class FileBackendReadWriteRaw(val directory: File, val name: String) extends Common[FileReadWrite]
  with ByteStore {
    val dirHelper = new DirWithConfig(FileBackend, directory)
    dirHelper markOpen()
    override def dataFile(dataDirectory: File, fileNumber: FileNumber, startPosition: Position): FileReadWrite =
      new FileReadWrite(namePrint, dataDirectory, fileNumber, startPosition)
    override def nextBlockStart(position: Long): Long = FileBackend.nextBlockStart(blocksize, Position(position)).value
    /** Note: In the current implementation of DataFile, up to data.length
      * bytes of RAM may be additionally allocated while writing. */
    @annotation.tailrec
    override def write(data: Bytes, at: Long): Unit = {
      val offsetInFile = Position(at) % blocksize
      val bytesToWriteInDataFile = math.min((blocksize - offsetInFile).value, data.length).toInt // Int because data.length is Int
      dataFiles(Position(at) / blocksize)(_ write(offsetInFile, data withLength bytesToWriteInDataFile))
      if (data.length > bytesToWriteInDataFile) write(data addOffset bytesToWriteInDataFile, at + bytesToWriteInDataFile)
    }
    override def clear(from: Long, to: Long): Unit =
      blockStream(Position(from), Size(to - from)).reverse foreach { // reverse makes clear more efficient
        case (dataFileNumber, offset, length) => dataFiles(dataFileNumber) { _ clear (offset, length) }
      }
    override def close(): Unit = { dataFiles close(); dirHelper markClosed() }
  }
}
