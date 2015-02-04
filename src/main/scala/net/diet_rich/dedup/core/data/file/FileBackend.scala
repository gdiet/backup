package net.diet_rich.dedup.core.data.file

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

import scala.collection.mutable.LinkedHashMap

class FileBackend(dataDir: File, repositoryId: String, readonly: Boolean) extends DataBackend {
  private val blocksize: Int = { // int to avoid problems with byte array size
    val settings = readSettingsFile(dataDir / settingsFile)
    require(settings(versionKey) == versionValue)
    require(settings(repositoryIDKey) == repositoryId)
    settings(blocksizeKey).toInt
  }
  private val maxReadSize: Int = math.min(blocksize, 65536)
  private val maxNumberOfOpenFiles: Int = 64

  private var dataFiles = LinkedHashMap.empty[Long, DataFile]

  private def dataFile(dataFileNumber: Long): DataFile =
    init(dataFiles.remove(dataFileNumber) getOrElse new DataFile(dataFileNumber, dataDir, readonly)) { dataFile =>
      if (dataFiles.size >= maxNumberOfOpenFiles) dataFiles.remove(dataFiles.keys.head).get.close()
      dataFiles += (dataFileNumber -> dataFile)
    }

  override def blockAligned(position: Long): Long = position % blocksize match {
    case 0L => position
    case modulo => position + blocksize - modulo
  }

  override def read(start: Long, size: Long): Iterator[Bytes] = synchronized {
    def blockStream(start: Long, size: Long): Stream[(Long, Int, Int)] = size match {
      case 0L => Stream.empty
      case remaining =>
        assert(remaining > 0)
        val dataFileNumber = start / blocksize
        val offsetInFile = start % blocksize toInt
        val bytesToRead = math.min(maxReadSize, math.min(blocksize - offsetInFile, remaining).toInt)
        (dataFileNumber, offsetInFile, bytesToRead) #:: blockStream(start + bytesToRead, remaining - bytesToRead)
    }
    blockStream(start, size)
      .iterator
      .map {case ((fileNumber, offset, length)) => dataFile(fileNumber).read(offset, length)}
  }

  override def write(data: Bytes, start: Long): Unit = synchronized {
    val dataFileNumber = start / blocksize
    val offsetInFile = start % blocksize toInt
    val bytesToWriteInDataFile = math.min(blocksize - offsetInFile, data.length)
    dataFile(dataFileNumber).writeData(offsetInFile, data setLength bytesToWriteInDataFile)
    if (data.length > bytesToWriteInDataFile) write(data addOffset bytesToWriteInDataFile, start + bytesToWriteInDataFile)
  }

  override def close(): Unit = synchronized {
    dataFiles.values foreach (_.close())
    dataFiles.clear()
  }
}
