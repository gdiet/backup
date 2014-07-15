// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.data

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.DAYS

import scala.concurrent.{ExecutionContext, Future}

import net.diet_rich.dedup.core.Lifecycle
import net.diet_rich.dedup.core.values.{Bytes, DataRange, Position, Size}
import net.diet_rich.dedup.util.resultOf

trait DataBackendSlice {
  trait DataBackend {
    def read(entry: DataRange): Iterator[Bytes]
    def write(data: Bytes, start: Position): Unit
  }
  def dataBackend: DataBackend
}

trait DataSettingsSlice {
  case class DataSettings(blocksize: Size, dataDir: File, storeThreads: Int, fileHandlesPerThread: Int, readonly: Boolean)
  def dataSettings: DataSettings
}

trait DataStorePart extends DataBackendSlice with Lifecycle { _: DataSettingsSlice =>
  abstract override def teardown() = {
    super.teardown()
    dataBackend teardown()
  }

  object dataBackend extends DataBackend {
    override def read(entry: DataRange): Iterator[Bytes] = {
      val initialDataFileNumber = entry.start / dataSettings.blocksize
      dataFileDistributionFor(initialDataFileNumber, entry.start, entry.size, Nil).iterator.map {
        case (dataFileNumber, offsetInFile, currentSize) =>
          execute(dataFileNumber){ _ read (offsetInFile.value, Bytes zero currentSize) }
      }
    }

    override def write(data: Bytes, start: Position): Unit = {
      val initialDataFileNumber = start / dataSettings.blocksize
      dataFileDistributionFor(initialDataFileNumber, start, data.size, Nil).foldLeft (data) {
        case (remainingData, (dataFileNumber, offsetInFile, currentSize)) =>
          val currentChunk = remainingData withSize currentSize
          val printOfCurrentChunk = DataFile.calcDataPrint(offsetInFile.value, currentChunk) // calculated outside of the store thread!
          execute(dataFileNumber){ _ writeData(offsetInFile.value, currentChunk, printOfCurrentChunk) }
          remainingData withOffset currentSize
      }
    }

    private[DataStorePart] def teardown() = {
      executors foreach {_ shutdown()}
      executors foreach {_ awaitTermination(1, DAYS)}
      dataFiles flatMap (_ values) foreach (_ close())
    }

    private val executors = Array.fill(dataSettings storeThreads)(Executors.newSingleThreadExecutor)
    private def threadNumber(dataFileNumber: Long): Int = (dataFileNumber % dataSettings.storeThreads).toInt
    private def executor(dataFileNumber: Long) = executors(threadNumber(dataFileNumber))
    private def execute[T](dataFileNumber: Long)(f: DataFile => T): T =
      resultOf(Future(f(dataFileHandler(dataFileNumber)))(ExecutionContext fromExecutorService executor(dataFileNumber)))

    private def pathInDataDir(dataFileNumber: Long) = f"$dataFileNumber%010X" grouped 2 mkString "/"
    private def dataFile(dataFileNumber: Long): File = new File(dataSettings.dataDir, pathInDataDir(dataFileNumber))

    private val dataFiles = Array.fill(dataSettings storeThreads)(scala.collection.mutable.LinkedHashMap[Long, DataFile]())
    private def dataFileHandler(dataFileNumber: Long): DataFile = {
      val dataFilesMap = dataFiles(threadNumber(dataFileNumber))
      val dataFileHandler = dataFilesMap remove dataFileNumber getOrElse new DataFile(dataFileNumber, dataFile(dataFileNumber), dataSettings readonly)
      if (dataFilesMap.size >= dataSettings.fileHandlesPerThread) dataFilesMap.remove(dataFilesMap.keys.head).get close()
      dataFilesMap.put(dataFileNumber, dataFileHandler)
      dataFileHandler
    }

    @annotation.tailrec
    private def dataFileDistributionFor(dataFileNumber: Long, start: Position, size: Size, acc: List[(Long, Position, Size)]): List[(Long, Position, Size)] = {
      val offsetInFile = start % dataSettings.blocksize
      val maxSizeInFile = dataSettings.blocksize - offsetInFile
      val currentSize = if (size > maxSizeInFile) maxSizeInFile else size
      val currentPart = (dataFileNumber, offsetInFile.asPosition, currentSize)
      if (size <= maxSizeInFile) (currentPart :: acc).reverse
      else dataFileDistributionFor(dataFileNumber + 1, start + currentSize, size - currentSize, currentPart :: acc)
    }
  }
}
