// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.data.DataBackendSlice
import net.diet_rich.dedup.core.values.{Print, Size, Hash, DataEntry, DataEntryID, DataRange, Bytes}
import net.diet_rich.dedup.util.init

trait DataHandlerSlice {
  trait DataHandler {
    def readData(entry: DataEntryID): Iterator[Bytes]
    def hasSizeAndPrint(size: Size, print: Print): Boolean
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry]
    def storePackedData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID
    def storeSourceData(data: Source): DataEntryID
    def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID
  }
  def dataHandler: DataHandler
}

trait DataHandlerPart extends DataHandlerSlice { _: DataBackendSlice with StoreSettingsSlice with FreeRangesSlice with sql.TablesPart =>
  final object dataHandler extends DataHandler {
    override def readData(entry: DataEntryID): Iterator[Bytes] =
      tables.storeEntries(entry).iterator.flatMap(dataEntry => dataBackend read dataEntry.range)

    override def hasSizeAndPrint(size: Size, print: Print): Boolean =
      !(tables dataEntries(size, print) isEmpty)

    override def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] =
      tables dataEntries(size, print, hash)

    override def storeSourceData(source: Source): DataEntryID = {
      val printData = source read FileSystem.PRINTSIZE
      storeSourceData(printData, Print(printData), source.allData, source.size)
    }

    override def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID = {
      val (hash, size, dataID) = Hash calculate(storeSettings.hashAlgorithm, Iterator(printData) ++ data, { data =>
        val packedData = storeSettings.storeMethod pack data
        storePackedDataAndCreateByteStoreEntries(packedData, estimatedSize)
      })
      createDataTableEntry(dataID, size, print, hash)
      dataID
    }

    override def storePackedData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID =
      init(storePackedDataAndCreateByteStoreEntries(data, size)) { dataID =>
        createDataTableEntry(dataID, size, print, hash)
      }

    // Note: In theory, a different thread might just have finished storing the same data and we create a data
    // duplicate here. I think however that is is preferrable to clean up data duplicates with a utility from
    // time to time and not care about them here. Only if we could be sure the utility is NOT needed, I'd go
    // for taking care about them here.
    private def createDataTableEntry(dataID: DataEntryID, size: Size, print: Print, hash: Hash) =
      tables.createDataEntry(dataID, size, print, hash, storeSettings.storeMethod)

    private def storePackedDataAndCreateByteStoreEntries(data: Iterator[Bytes], estimatedSize: Size): DataEntryID = {
      val storedRanges = storePackedData(data, estimatedSize)
      init(tables.nextDataID) { dataID =>
        storedRanges foreach {
          tables.createByteStoreEntry(dataID, _)
        }
      }
    }

    private sealed trait RangesOrUnstored
    private case class Ranges(remaining: List[DataRange]) extends RangesOrUnstored
    private case class Unstored(unstored: List[Bytes]) extends RangesOrUnstored

    private def storePackedData(data: Iterator[Bytes], estimatedSize: Size): List[DataRange] = {
      val storeRanges = freeRanges dequeue estimatedSize
      data.foldLeft[RangesOrUnstored](Ranges(storeRanges)) { case (ranges, bytes) => storeOneChunk(bytes, ranges)} match {
        case Unstored(additionalData) =>
          storeRanges ::: storePackedData(additionalData.iterator, additionalData.totalSize)
        case Ranges(remaining) =>
          remaining foreach freeRanges.enqueue
          remaining match {
            case Nil => storeRanges
            case partialRestRange :: unstoredRanges =>
              val fullyStored :+ partiallyStored = storeRanges dropRight unstoredRanges.size
              fullyStored :+ (partiallyStored shortenBy partialRestRange.size)
          }
      }
    }

    @annotation.tailrec
    private def storeOneChunk(bytes: Bytes, ranges: RangesOrUnstored): RangesOrUnstored = ranges match {
      case Unstored(data) => Unstored(bytes :: data)
      case Ranges(Nil) => Unstored(List(bytes))
      case Ranges(head :: tail) =>
        head.size - bytes.size match {
          case Size.Negative(_) =>
            dataBackend.write(bytes withSize head.size, head.start)
            storeOneChunk(bytes withOffset head.size, Ranges(tail))
          case Size.Zero =>
            dataBackend.write(bytes, head.start)
            Ranges(tail)
          case _ =>
            dataBackend.write(bytes, head.start)
            Ranges((head withOffset bytes.size) :: tail)
        }
    }
  }
}
