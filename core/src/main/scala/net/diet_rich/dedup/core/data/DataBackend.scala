// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.data

import java.io.File

import net.diet_rich.dedup.core.values.{Bytes, DataRange, Position, Size}

trait DataBackendSlice {
  trait DataBackend {
    def read(entry: DataRange): Iterator[Bytes]
    def write(data: Bytes, start: Position): Unit
  }
  def dataBackend: DataBackend
}

trait DataSettingsSlice {
  case class DataSettings(blocksize: Size, dataDir: File)
  def dataSettings: DataSettings
}

trait DataStorePart extends DataBackendSlice { _: DataSettingsSlice =>
  object dataBackend extends DataBackend {
    override def read(entry: DataRange): Iterator[Bytes] = {
      val blocks = partitionAreaAtBlockSize(entry.start, entry.size)
      ???
    }
    override def write(data: Bytes, start: Position): Unit = {
      partitionAreaAtBlockSize(start, data.size) foreach { block =>
        val (dataFileNumber, offsetInDataFile) = dataFileNumberAndOffset(block.start)
      }
    }

    private def dataFileNumberAndOffset(position: Position): (Long, Size) =
      (position / dataSettings.blocksize, position % dataSettings.blocksize)

    private def partitionAreaAtBlockSize(start: Position, size: Size): List[DataRange] = {
      @annotation.tailrec
      def partitionAreaAtBlockSizeReverted(start: Position, size: Size, acc: List[DataRange]): List[DataRange] = {
        val offset = start % dataSettings.blocksize
        if (offset + size <= dataSettings.blocksize) DataRange(start, size) :: acc
        else partitionAreaAtBlockSizeReverted(start, size, DataRange(start, dataSettings.blocksize - offset) :: acc)
      }
      partitionAreaAtBlockSizeReverted(start, size, Nil).reverse
    }
  }
}
