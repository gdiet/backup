// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataHandlerSlice {
  protected sealed trait DataEntryCreateResult { val id: DataEntryID }
  protected case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
  protected case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult
  trait DataHandler {
    def hasSizeAndPrint(size: Size, print: Print): Boolean
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry]
    def reserveDataID: DataEntryID
    def storeData(data: Seq[Bytes]): DataEntryID
    def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult
    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit
    def initialDataOverlapProblems: List[(StoreEntry, StoreEntry)]
    def freeRanges(size: Size): Seq[DataRange]
    def nextFreeRange: DataRange
    def requeueFreeRange (range: DataRange): Unit
    def readData(entry: DataEntryID): Iterator[Bytes] // FIXME remove and make this a data store slice
  }
  protected val data: DataHandler
}

trait DataHandlerPart extends DataHandlerSlice with DataBackendPart { _: sql.TablesSlice =>
  override protected val data = new DataHandler() {
    def hasSizeAndPrint(size: Size, print: Print): Boolean = !(tables dataEntries(size, print) isEmpty)
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = tables dataEntries(size, print, hash)
    def reserveDataID: DataEntryID = tables.nextDataID
    def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult = tables.inTransaction {
      tables.dataEntries(size, print, hash).headOption
        .map(e => ExistingEntryMatches(e.id))
        .getOrElse(DataEntryCreated(tables.createDataEntry(size, print, hash, method)))
    }

    def storeData(data: Seq[Bytes]): DataEntryID = ??? // FIXME
    //  val packedSize = packedData.foldLeft(Size(0)) { case (size, bytes) => size + bytes.size }
    //  val dataId = data.reserveDataID
    //  val dataRanges = data freeRanges packedSize // FIXME move to DataHandlerSlice
    //  // store bytes
    //  // store ranges
    //  // store dataid
    //  ???
    //    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
    //      case ExistingEntryMatches(dataid) => dataid
    //      case DataEntryCreated(dataid) =>
    //        val rangesStored = storeMethod pack data.iterator flatMap { storeBytes(_).reverse }
    //        rangesStored foreach (createByteStoreEntry(dataid, _))
    //        dataid
    //    }
    //
    //    @annotation.tailrec
    //    protected final def storeBytes(bytes: Bytes, acc: List[DataRange] = Nil): List[DataRange] = {
    //      val (block, rest) = nextFreeRange partitionAtLimit bytes.size
    //      dataBackend.write(bytes, block)
    //      if (block.size < bytes.size) {
    //        assume (rest isEmpty)
    //        storeBytes(bytes withOffset block.size, block :: acc)
    //      } else {
    //        rest foreach requeueFreeRange
    //        block :: acc
    //      }
    //    }

    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit = tables.createByteStoreEntry(dataid, range)

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

    def freeRanges(size: Size): Seq[DataRange] = freeRangesQueue.synchronized {
      @annotation.tailrec
      def collectFreeRanges(size: Size, ranges: List[DataRange]): List[DataRange] = {
        freeRangesQueue dequeue() partitionAt size match {
          case ExactMatch(range) =>
            range :: ranges
          case NotLargeEnough(range, remainingSize) =>
            collectFreeRanges(remainingSize, range :: ranges)
          case WithRest(range, rest) =>
            freeRangesQueue enqueue rest
            range :: ranges
        }
      }
      if (size == Size(0)) Nil else collectFreeRanges(size, Nil)
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
