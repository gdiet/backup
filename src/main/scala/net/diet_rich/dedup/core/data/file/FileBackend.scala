package net.diet_rich.dedup.core.data.file

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

import scala.collection.mutable

object FileBackend {
  def apply(dataDir: File, repositoryId: String, readonly: Boolean): DataBackend = new FileBackend(dataDir, repositoryId, readonly)
}

class FileBackend private(dataDir: File, repositoryId: String, readonly: Boolean) extends DataBackend {
  private val blocksize: Int = { // int to avoid problems with byte array size
    val settings = readSettingsFile(dataDir / settingsFile)
    require(settings(versionKey) == versionValue)
    require(settings(repositoryIDKey) == repositoryId)
    settings(blocksizeKey).toInt
  }
  private val maxReadSize: Int = math.min(blocksize, 65536)
  private val maxNumberOfOpenFiles: Int = 64

  private object DataFiles {
    private val dataFiles = mutable.LinkedHashMap.empty[Long, DataFile]
    def apply(dataFileNumber: Long): DataFile = synchronized {
      init(dataFiles.remove(dataFileNumber) getOrElse new DataFile(dataFileNumber, dataDir, readonly)) { dataFile =>
        if (dataFiles.size >= maxNumberOfOpenFiles) dataFiles.remove(dataFiles.keys.head).get.close()
        dataFiles += (dataFileNumber -> dataFile)
      }
    }
    def apply(f: mutable.LinkedHashMap[Long, DataFile] => Unit): Unit = synchronized { f }
  }

  override def nextBlockStart(position: Long): Long = position + blocksize - position % blocksize

  override def read(start: Long, size: Long): Iterator[Bytes] = {
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
      .map {case ((fileNumber, offset, length)) => DataFiles(fileNumber).read(offset, length)}
  }

  override def write(data: Bytes, start: Long): Unit = {
    val dataFileNumber = start / blocksize
    val offsetInFile = start % blocksize toInt
    val bytesToWriteInDataFile = math.min(blocksize - offsetInFile, data.length)
    DataFiles(dataFileNumber).writeData(offsetInFile, data setLength bytesToWriteInDataFile)
    if (data.length > bytesToWriteInDataFile) write(data addOffset bytesToWriteInDataFile, start + bytesToWriteInDataFile)
  }

  override def close(): Unit = DataFiles { all =>
    all.values foreach (_.close())
    all.clear()
  }
}
