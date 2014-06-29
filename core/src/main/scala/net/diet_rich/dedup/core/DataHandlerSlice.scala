// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataHandlerSlice extends sql.TablesSlice with DataBackendPart {
  protected sealed trait DataEntryCreateResult { val id: DataEntryID }
  protected case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
  protected case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult

  protected object data {
    def hasSizeAndPrint(size: Size, print: Print): Boolean = !(tables dataEntries(size, print) isEmpty)
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = tables dataEntries(size, print, hash)
    def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult = tables.inTransaction {
      tables.dataEntries(size, print, hash).headOption
        .map(e => ExistingEntryMatches(e.id))
        .getOrElse(DataEntryCreated(tables.createDataEntry(size, print, hash, method)))
    }

    val createByteStoreEntry = tables.createByteStoreEntry _

    val initialDataOverlapProblems = tables.problemDataAreaOverlaps

    // Note: We could use a PriorityQueue here - however, it is not really necessary, an ordinary queue 'heals' itself here, too
    protected val freeRangesQueue = scala.collection.mutable.Queue[DataRange](DataRange(tables.startOfFreeDataArea, Position(Long MaxValue)))

    // FIXME chunk partitioning in the data backend
    val blocksize = Size(0x800000L)

    // queue gaps in byte store
    if (initialDataOverlapProblems.isEmpty) {
      val dataAreaStarts = tables.dataAreaStarts
      if (!dataAreaStarts.isEmpty) {
        val (firstArea :: gapStarts) = dataAreaStarts
        if (firstArea > Position(0L)) freeRangesQueue enqueue DataRange(Position(0L), firstArea)
        freeRangesQueue enqueue ((tables.dataAreaEnds zip gapStarts).map(DataRange.tupled):_*)
      }
    }

    def nextFreeRange: DataRange = freeRangesQueue.synchronized {
      val (range, rest) = freeRangesQueue.dequeue().partitionAtBlockLimit(blocksize)
      rest foreach (freeRangesQueue.enqueue(_))
      range
    }
    def requeueFreeRange (range: DataRange): Unit = freeRangesQueue.synchronized {
      freeRangesQueue enqueue range
    }

    def readData(entry: DataEntryID): Iterator[Bytes] =
      tables.storeEntries(entry).iterator.flatMap(dataEntry => dataBackend read dataEntry.range)
  }
}
