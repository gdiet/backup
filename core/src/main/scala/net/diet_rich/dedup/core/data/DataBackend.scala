// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.data

import java.io.File
import java.util.concurrent.Executors

import net.diet_rich.dedup.core.values.Size.Negative

import scala.concurrent.{ExecutionContext, Future}

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
  case class DataSettings(blocksize: Size, dataDir: File, storeThreads: Int)
  def dataSettings: DataSettings
}

trait DataStorePart extends DataBackendSlice { _: DataSettingsSlice =>
  object dataBackend extends DataBackend {
    override def read(entry: DataRange): Iterator[Bytes] = {
      ???
    }
    override def write(data: Bytes, start: Position): Unit = {
      val initialDataFileNumber = start / dataSettings.blocksize
      dataFileDistributionFor(initialDataFileNumber, start, data.size, Nil).foldLeft (data) {
        case (data, (dataFileNumber, offsetInFile, currentSize)) =>
          execute(dataFileNumber) {
            ???
          }
          data withOffset currentSize
      }
    }

    private val executors = Array.fill(dataSettings storeThreads)(Executors.newSingleThreadExecutor)
    private def executor(dataFileNumber: Long) = executors((dataFileNumber % dataSettings.storeThreads) toInt)
    private def execute[T](dataFileNumber: Long)(f: => T): T = resultOf(Future(f)(ExecutionContext fromExecutorService executor(dataFileNumber)))

    @annotation.tailrec
    private def dataFileDistributionFor(dataFileNumber: Long, start: Position, size: Size, acc: List[(Long, Position, Size)]): List[(Long, Position, Size)] = {
      val offsetInFile = start % dataSettings.blocksize
      val maxSizeInFile = dataSettings.blocksize - offsetInFile
      val currentSize = if (size > maxSizeInFile) maxSizeInFile else size
      val currentPart = (dataFileNumber, offsetInFile asPosition, currentSize)
      if (size <= maxSizeInFile) (currentPart :: acc) reverse
      else dataFileDistributionFor(dataFileNumber + 1, start + currentSize, size - currentSize, currentPart :: acc)
    }
  }
}
