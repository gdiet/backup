// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.data.DataBackendSlice
import net.diet_rich.dedup.core.values.{Print, Size, Hash, DataEntry, DataEntryID, DataRange, Bytes}
import net.diet_rich.dedup.util.{Equal, init}

trait DataHandlerSlice {
  trait DataHandler {
    def readData(entry: DataEntryID): Iterator[Bytes]
    def hasSizeAndPrint(size: Size, print: Print): Boolean
    def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry]
    def storeSourceData(data: Source): DataEntryID
    def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID
    def storeSourceData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID
  }
  def dataHandler: DataHandler
}

trait DataHandlerPart extends DataHandlerSlice { _: DataBackendSlice with StoreSettingsSlice with FreeRangesSlice with sql.TablesPart =>
  final object dataHandler extends DataHandler {
    import DataHandlerPart._
    override def readData(dataEntryID: DataEntryID): Iterator[Bytes] =
      tables.dataEntry(dataEntryID).iterator flatMap {
        _.method unpack (tables.storeEntries(dataEntryID).iterator flatMap (dataBackend read _.range))
      }

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

    override def storeSourceData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
      val packedData = storeSettings.storeMethod.pack(data)
      init(storePackedDataAndCreateByteStoreEntries(data, size)) { dataID =>
        createDataTableEntry(dataID, size, print, hash)
      }
    }

    // Note: In theory, a different thread might just have finished storing the same data and we create a data
    // duplicate here. I think however that is is preferrable to clean up data duplicates with a utility from
    // time to time and not care about them here. Only if we could be sure the utility is NOT needed, I'd go
    // for taking care about them here.
    private def createDataTableEntry(dataID: DataEntryID, size: Size, print: Print, hash: Hash) =
      tables.createDataEntry(dataID, size, print, hash, storeSettings.storeMethod)

    private def storePackedDataAndCreateByteStoreEntries(data: Iterator[Bytes], estimatedSize: Size): DataEntryID = {
      val storedRanges = storePackedData(data, estimatedSize)
      // FIXME make a utility method and test
      val normalizedRanges = storedRanges.foldLeft(List.empty[DataRange]) {
        case (Nil, range) => List(range)
        case (head :: tail, range) if head.fin === range.start => DataRange(head.start, range.fin) :: tail
        case (ranges, range) => range :: ranges
      }.reverse
      init(tables.nextDataID) { dataID =>
        normalizedRanges foreach {
          tables.createByteStoreEntry(dataID, _)
        }
      }
    }

    private def storePackedData(data: Iterator[Bytes], estimatedSize: Size): List[DataRange] = {
      val storeRanges = freeRanges dequeueAtLeast estimatedSize
      val protocol = data.foldLeft[RangesOrUnstored](Ranges(Nil, storeRanges)) {
        case (ranges, Bytes(_, _, 0)) => ranges
        case (ranges, bytes) => storeOneChunk(bytes, ranges)
      }
      protocol match {
        case Unstored(additionalData) =>
          storeRanges ::: storePackedData(additionalData.iterator, additionalData.sizeInBytes)
        case Ranges(stored, remaining) =>
          remaining foreach freeRanges.enqueue
          stored.reverse
      }
    }

    @annotation.tailrec
    private def storeOneChunk(bytes: Bytes, ranges: RangesOrUnstored): RangesOrUnstored = ranges match {
      case Unstored(data) => Unstored(bytes :: data)
      case Ranges(_, Nil) => Unstored(List(bytes))
      case Ranges(stored, head :: tail) =>
        head.size - bytes.size match {
          case Size.Negative(_) =>
            dataBackend.write(bytes withSize head.size, head.start)
            storeOneChunk(bytes withOffset head.size, Ranges(head :: stored, tail))
          case Size.Zero =>
            dataBackend.write(bytes, head.start)
            Ranges(head :: stored, tail)
          case _ =>
            dataBackend.write(bytes, head.start)
            Ranges((head withLength bytes.size) :: stored, (head withOffset bytes.size) :: tail)
        }
    }
  }
}

object DataHandlerPart {
  private sealed trait RangesOrUnstored
  private final case class Ranges(stored: List[DataRange], remaining: List[DataRange]) extends RangesOrUnstored
  private final case class Unstored(unstored: List[Bytes]) extends RangesOrUnstored
}
