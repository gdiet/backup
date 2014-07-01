// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataHandlerSlice {
  trait DataHandler {
    def readData(entry: DataEntryID): Iterator[Bytes] // FIXME remove and make this a data store slice?
    def hasSizeAndPrint(size: Size, print: Print): Boolean
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry]
    def storePackedData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID
    def storeSourceData(data: Iterator[Bytes]): DataEntryID
    def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes]): DataEntryID
  }
  protected val dataHandler: DataHandler
}

trait DataHandlerPart extends DataHandlerSlice with DataBackendPart { _: sql.TablesSlice =>
  private sealed trait DataEntryCreateResult { val id: DataEntryID }
  private case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
  private case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult

  override protected val dataHandler = new DataHandler() {

    override def readData(entry: DataEntryID): Iterator[Bytes] = tables.storeEntries(entry).iterator.flatMap(dataEntry => dataBackend read dataEntry.range)

    override def hasSizeAndPrint(size: Size, print: Print): Boolean = !(tables dataEntries(size, print) isEmpty)

    override def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = tables dataEntries(size, print, hash)

    override def storePackedData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
      val storeRanges = freeRanges(size)
      @annotation.tailrec
      def storeOneChunk(bytes: Bytes, ranges: List[DataRange]): List[DataRange] = ranges.head.partitionAt(bytes.size) match {
        case WithRest(range, rest) =>
          dataBackend.write(bytes, range.start)
          rest :: ranges.tail
        case ExactMatch(range) =>
          dataBackend.write(bytes, range.start)
          ranges.tail
        case NotLargeEnough(range, missing) =>
          dataBackend.write(bytes.withSize(range.size), range.start)
          storeOneChunk(bytes.withOffset(range.size), ranges.tail)
      }
//        bytes.size match {
//        case Size(0) => ranges
//        case size if size == ranges.head.size =>
//          dataBackend.write(bytes, ranges.head)
//          ranges.tail
//        case size if size <= ranges.head.size =>
//          ranges.head.partitionAt(size)
//
//          ???
//        case _ => ???
//      }

      val reservedDataID = tables.nextDataID
      ???
    }

    override def storeSourceData(data: Iterator[Bytes]): DataEntryID = ???

    override def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes]): DataEntryID = ???

    // Note: We could use a PriorityQueue here - however, it is not really necessary, an ordinary queue 'heals' itself here, too
    val freeRangesQueue = scala.collection.mutable.Queue[DataRange](DataRange(tables.startOfFreeDataArea, Position(Long MaxValue)))

    def freeRanges(size: Size): List[DataRange] = freeRangesQueue.synchronized {
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


    //    def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult = tables.inTransaction {
    //      tables.dataEntries(size, print, hash).headOption
    //        .map(e => ExistingEntryMatches(e.id))
    //        .getOrElse(DataEntryCreated(tables.createDataEntry(size, print, hash, method)))
    //    }
    //
    //    def storeData(data: Seq[Bytes]): DataEntryID = ??? // FIXME
    //    //  val packedSize = packedData.foldLeft(Size(0)) { case (size, bytes) => size + bytes.size }
    //    //  val dataId = data.reserveDataID
    //    //  val dataRanges = data freeRanges packedSize // FIXME move to DataHandlerSlice
    //    //  // store bytes
    //    //  // store ranges
    //    //  // store dataid
    //    //  ???
    //    //    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
    //    //      case ExistingEntryMatches(dataid) => dataid
    //    //      case DataEntryCreated(dataid) =>
    //    //        val rangesStored = storeMethod pack data.iterator flatMap { storeBytes(_).reverse }
    //    //        rangesStored foreach (createByteStoreEntry(dataid, _))
    //    //        dataid
    //    //    }
    //    //
    //    //    @annotation.tailrec
    //    //    protected final def storeBytes(bytes: Bytes, acc: List[DataRange] = Nil): List[DataRange] = {
    //    //      val (block, rest) = nextFreeRange partitionAtLimit bytes.size
    //    //      dataBackend.write(bytes, block)
    //    //      if (block.size < bytes.size) {
    //    //        assume (rest isEmpty)
    //    //        storeBytes(bytes withOffset block.size, block :: acc)
    //    //      } else {
    //    //        rest foreach requeueFreeRange
    //    //        block :: acc
    //    //      }
    //    //    }
    //
    //    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit = tables.createByteStoreEntry(dataid, range)
    //
    //    val initialDataOverlapProblems = tables.problemDataAreaOverlaps
    //
    //    // FIXME chunk partitioning in the data backend
    //    val blocksize = Size(0x800000L)
    //
    //    // queue gaps in byte store
    //    if (initialDataOverlapProblems.isEmpty) {
    //      val dataAreaStarts = tables.dataAreaStarts
    //      if (!dataAreaStarts.isEmpty) {
    //        val (firstArea :: gapStarts) = dataAreaStarts
    //        if (firstArea > Position(0L)) freeRangesQueue enqueue DataRange(Position(0L), firstArea)
    //        freeRangesQueue enqueue ((tables.dataAreaEnds zip gapStarts).map(DataRange.tupled):_*)
    //      }
    //    }
    //
    //    def nextFreeRange: DataRange = freeRangesQueue.synchronized {
    //      val (range, rest) = freeRangesQueue.dequeue().partitionAtBlockLimit(blocksize)
    //      rest foreach (freeRangesQueue.enqueue(_))
    //      range
    //    }
    //    def requeueFreeRange (range: DataRange): Unit = freeRangesQueue.synchronized {
    //      freeRangesQueue enqueue range
    //    }
  }
}
