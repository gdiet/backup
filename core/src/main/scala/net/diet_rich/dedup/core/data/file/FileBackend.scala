package net.diet_rich.dedup.core.data.file

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

import scala.collection.mutable

object FileBackend {
  def create(dataDir: File, repositoryid: String, blocksize: Int) = {
    require(dataDir mkdir(), s"Can't create data directory $dataDir")
    val settings = Map(
      dataVersionKey -> dataVersionValue,
      repositoryidKey -> repositoryid,
      dataBlocksizeKey -> s"$blocksize"
    )
    writeSettingsFile(dataDir / dataSettingsFile, settings)
  }

  def nextBlockStart(position: Long, blocksize: Long): Long = position % blocksize match {
    case 0 => position
    case n => position + blocksize - n
  }
}

class FileBackend (dataDir: File, repositoryid: String, readonly: Boolean) extends DataBackend {
  val blocksize: Int = { // int to avoid problems with byte array size
    val settings = readSettingsFile(dataDir / dataSettingsFile)
    require(settings(dataVersionKey) == dataVersionValue)
    require(settings(repositoryidKey) == repositoryid)
    settings(dataBlocksizeKey).toInt
  }
  private val maxReadSize: Int = math.min(blocksize, 65536)
  private val maxNumberOfOpenFiles: Int = 64

  private object DataFiles {
    private val dataFiles = mutable.LinkedHashMap.empty[Long, DataFile]
    def apply(fileNumber: Long): DataFile = synchronized {
      init(dataFiles.remove(fileNumber) getOrElse new DataFile(fileNumber, fileNumber * blocksize, dataDir, readonly)) { dataFile =>
        if (dataFiles.size >= maxNumberOfOpenFiles) dataFiles.remove(dataFiles.keys.head).get.close()
        dataFiles += (fileNumber -> dataFile)
      }
    }
    def apply(f: mutable.LinkedHashMap[Long, DataFile] => Unit): Unit = synchronized { f }
  }

  override def nextBlockStart(position: Long): Long = FileBackend.nextBlockStart(position, blocksize)

  override def read(range: StartFin): Iterator[Bytes] = {
    def blockStream(start: Long, size: Long): Stream[(Long, Int, Int)] = // returns: data file number / offset in file / bytes to read
      if (size <= 0L) { assert (size == 0L); Stream.empty }
      else {
        val dataFileNumber = start / blocksize
        val offsetInFile = start % blocksize toInt
        val bytesToRead = math.min(maxReadSize, math.min(blocksize - offsetInFile, size).toInt)
        (dataFileNumber, offsetInFile, bytesToRead) #:: blockStream(start + bytesToRead, size - bytesToRead)
      }
    val (start, fin) = range
    blockStream(start, fin - start).iterator
      .map {case ((fileNumber, offset, length)) => DataFiles(fileNumber).read(offset, length)}
  }

  override def write(data: Bytes, start: Long): Unit = {
    val dataFileNumber = start / blocksize
    val offsetInFile = start % blocksize toInt
    val bytesToWriteInDataFile = math.min(blocksize - offsetInFile, data.length)
    DataFiles(dataFileNumber).writeData(offsetInFile, data copy (length = bytesToWriteInDataFile))
    if (data.length > bytesToWriteInDataFile) write(data addOffset bytesToWriteInDataFile, start + bytesToWriteInDataFile)
  }

  override def close(): Unit = DataFiles { all =>
    all.values foreach (_.close())
    all.clear()
  }
}