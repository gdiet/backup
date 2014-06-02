// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util._

/** public: needed by other parts of the backup system; protected: access needed for testing */
abstract class FileSystemData(sqlTables: SQLTables, protected val dataSettings: DataSettings) {
  import dataSettings.blocksize

  def hasSizeAndPrint(size: Size, print: Print): Boolean = !(sqlTables dataEntries(size, print) isEmpty)
  def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = sqlTables dataEntries(size, print, hash)
  def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult = sqlTables.inWriteContext {
    sqlTables.dataEntries(size, print, hash).headOption
      .map(e => ExistingEntryMatches(e.id))
      .getOrElse(DataEntryCreated(sqlTables.createDataEntry(size, print, hash, method)))
  }

  val createByteStoreEntry = sqlTables.createByteStoreEntry _

  val initialDataOverlapProblems = sqlTables.problemDataAreaOverlaps

  protected val freeRangesQueue = scala.collection.mutable.Queue[DataRange](DataRange(sqlTables.startOfFreeDataArea, Position(Long MaxValue)))

  // queue gaps in byte store
  if (initialDataOverlapProblems.isEmpty) {
    val dataAreaStarts = sqlTables.dataAreaStarts
    if (!dataAreaStarts.isEmpty) {
      val (firstArea :: gapStarts) = dataAreaStarts
      if (firstArea > Position(0L)) freeRangesQueue enqueue DataRange(Position(0L), firstArea)
      freeRangesQueue enqueue ((sqlTables.dataAreaEnds zip gapStarts).map(DataRange.tupled):_*)
    }
  }

  def nextFreeRange: DataRange = synchronized {
    val (range, rest) = freeRangesQueue.dequeue().partitionAtBlockLimit(blocksize)
    rest foreach (freeRangesQueue.enqueue(_))
    range
  }
  def requeueFreeRange (range: DataRange): Unit = synchronized {
    freeRangesQueue enqueue range
  }

  def writeData(data: Bytes, range: DataRange): Unit

}
