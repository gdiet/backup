// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values._

trait DataHandlerSlice {
  trait DataHandler {
    def readData(entry: DataEntryID): Iterator[Bytes]
    def hasSizeAndPrint(size: Size, print: Print): Boolean
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry]
    def storePackedNewData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID
    def storeSourceData(data: Source): DataEntryID
    def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID
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

    private sealed trait RangesOrUnstored
    private case class Ranges (remaining: List[DataRange]) extends RangesOrUnstored
    private case class Unstored (unstored: List[Bytes]) extends RangesOrUnstored

    override def storePackedNewData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
      val storeRanges = freeRanges dequeue size
      val remainingRanges = data.foldLeft[RangesOrUnstored](Ranges(storeRanges)){case (ranges, bytes) => storeOneChunk(bytes, ranges)}
      assert (remainingRanges.asInstanceOf[Ranges].remaining isEmpty) // FIXME remove eventually?

      val reservedDataID = tables.nextDataID
      storeRanges foreach { range => tables.createByteStoreEntry(reservedDataID, range) }

      // Note: In theory, a different thread might just have finished storing the same data and we create a data
      // duplicate here. I think however that is is preferrable to clean up data duplicates with a utility from
      // time to time and not care about them here. Only if we could be sure the utility is NOT needed, I'd go
      // for taking care about them here.
      tables.createDataEntry(reservedDataID, size, print, hash, storeSettings.storeMethod)
      reservedDataID
    }

    // FIXME needs test!
    private def storeData(data: Iterator[Bytes], estimatedSize: Size): List[DataRange] = {
      val storeRanges = freeRanges dequeue estimatedSize
      data.foldLeft[RangesOrUnstored](Ranges(storeRanges)){case (ranges, bytes) => storeOneChunk(bytes, ranges)} match {
        case Ranges(remaining) =>
          remaining foreach freeRanges.enqueue
          remaining match {
            case Nil => storeRanges
            case partialRange :: fullUnstoredRanges =>
              val fullyStored :+ partiallyStored = storeRanges dropRight fullUnstoredRanges.size
              fullyStored :+ (partiallyStored shortenBy partialRange.size)
          }
        case Unstored(additionalData) =>
          val size = additionalData.map(_.size).foldLeft(Size.Zero)(_+_)
          storeRanges ::: storeData(additionalData.iterator, size)
      }
    }

    // FIXME needs test!
    @annotation.tailrec
    private def storeOneChunk(bytes: Bytes, ranges: RangesOrUnstored): RangesOrUnstored = ranges match {
      case Unstored(data) => Unstored(bytes :: data)
      case Ranges(Nil) => Unstored(List(bytes))
      case Ranges(head::tail) =>
        head.size - bytes.size match {
          case Size.Negative(_) =>
            dataBackend.write (bytes withSize head.size, head.start)
            storeOneChunk(bytes withOffset head.size, Ranges(tail))
          case Size.Zero =>
            dataBackend.write (bytes, head.start)
            Ranges (tail)
          case _ =>
            dataBackend.write (bytes, head.start)
            Ranges((head withOffset bytes.size) :: tail)
      }
    }

    override def storeSourceData(source: Source): DataEntryID = {
      val printData = source read FileSystem.PRINTSIZE
      val print = Print(printData)
      storeSourceData(printData, print, source.allData, source.size)
    }

    override def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID = {
      val (hash, size, dataID) = Hash calculate (storeSettings.hashAlgorithm, Iterator(printData) ++ data, data => {
        val packedData = storeSettings.storeMethod pack data
        val storeRanges = freeRanges dequeue estimatedSize // FIXME extract into same method
        val remainingRanges = data.foldLeft[RangesOrUnstored](Ranges(storeRanges)){case (ranges, bytes) => storeOneChunk(bytes, ranges)} // FIXME extract method

        remainingRanges match { // FIXME include into method
          case Ranges(Nil) => /* NOP */
          case Ranges(ranges) => freeRanges enqueue ranges
          case Unstored(data) =>
            val size = data.map(_.size).reduce(_+_) // FIXME or foldLeft(Size.Zero)(_+_) - this would also work if list is empty
            val storeRanges = freeRanges dequeue size
            val remainingRanges = data.foldLeft[RangesOrUnstored](Ranges(storeRanges)){case (ranges, bytes) => storeOneChunk(bytes, ranges)} // FIXME extract method
        }
        val reservedDataID = tables.nextDataID
        storeRanges foreach { range => tables.createByteStoreEntry(reservedDataID, range) }

        reservedDataID
      })

      // Note: In theory, a different thread might just have finished storing the same data and we create a data
      // duplicate here. I think however that is is preferrable to clean up data duplicates with a utility from
      // time to time and not care about them here. Only if we could be sure the utility is NOT needed, I'd go
      // for taking care about them here.
      tables.createDataEntry(dataID, size, print, hash, storeSettings.storeMethod)
      dataID
    }

    // FIXME it should be possible to initialize the free ranges with free slots read from the database
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
