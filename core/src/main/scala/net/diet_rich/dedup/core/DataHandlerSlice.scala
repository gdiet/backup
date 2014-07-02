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
    def storePackedNewData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID
    def storeSourceData(data: Iterator[Bytes]): DataEntryID
    def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes]): DataEntryID
  }
  protected val dataHandler: DataHandler
}

trait DataHandlerPart extends DataHandlerSlice with DataBackendPart { _: StoreSettingsSlice with sql.TablesSlice =>
  private sealed trait DataEntryCreateResult
  private object DataEntryCreated extends DataEntryCreateResult
  private case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult

  override protected val dataHandler = new DataHandler() {

    override def readData(entry: DataEntryID): Iterator[Bytes] = tables.storeEntries(entry).iterator.flatMap(dataEntry => dataBackend read dataEntry.range)

    override def hasSizeAndPrint(size: Size, print: Print): Boolean = !(tables dataEntries(size, print) isEmpty)

    override def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = tables dataEntries(size, print, hash)

    override def storePackedNewData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
      val storeRanges = freeRanges dequeue size
      @annotation.tailrec
      def storeOneChunk(bytes: Bytes, ranges: List[DataRange]): List[DataRange] = ranges.head.partitionAt(bytes.size) match {
        case WithRest(range, rest) =>
          assert(range.size == bytes.size) // FIXME remove eventually?
          dataBackend.write(bytes, range.start)
          rest :: ranges.tail
        case ExactMatch(range) =>
          assert(range.size == bytes.size) // FIXME remove eventually?
          dataBackend.write(bytes, range.start)
          ranges.tail
        case NeedsMore(range, missing) =>
          assert(range.size == bytes.size) // FIXME remove eventually?
          dataBackend.write(bytes.withSize(range.size), range.start)
          storeOneChunk(bytes.withOffset(range.size), ranges.tail)
      }
      val remainingRanges = data.foldLeft(storeRanges){ case (ranges, bytes) => storeOneChunk(bytes, ranges) }
      assert (remainingRanges isEmpty) // FIXME remove eventually?

      val reservedDataID = tables.nextDataID
      storeRanges foreach { range => tables.createByteStoreEntry(reservedDataID, range) }

      tables.createDataEntry(reservedDataID, size, print, hash, storeSettings.storeMethod)
      reservedDataID
    }

    override def storeSourceData(data: Iterator[Bytes]): DataEntryID = ???

    override def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes]): DataEntryID = ???

    val freeRanges = RangesQueue(DataRange(tables.startOfFreeDataArea, Position(Long MaxValue)))

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
