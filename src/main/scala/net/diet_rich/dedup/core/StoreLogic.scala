package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.Bytes
import net.diet_rich.dedup.core.values.DataRange

import scala.collection.mutable
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

import net.diet_rich.dedup.core.data.{StoreMethod, Hash, Bytes, Print}
import net.diet_rich.dedup.core.meta.{RangesQueue, MetaBackend}
import net.diet_rich.dedup.util._

trait StoreLogicBackend {
  def dataidFor(source: Source): Long
  def dataidFor(printData: Bytes, print: Long, source: Source): Long
  def close(): Unit
}

class StoreLogic(metaBackend: MetaBackend, freeRanges: RangesQueue, hashAlgorithm: String, storeMethod: Int, storeThreads: Int) extends StoreLogicBackend {

  private val storeExecutor = BlockingThreadPoolExecutor(storeThreads)
  private val storeContext = ExecutionContext fromExecutorService storeExecutor
  private def inStoreContext[T] (f: => T): T = Await result (Future(f)(storeContext), 1 day)

  def close(): Unit = storeExecutor shutdownAndAwaitTermination()

  def dataidFor(source: Source): Long = inStoreContext {
    val printData = source read Repository.PRINTSIZE
    dataidFor(printData, Print(printData), source)
  }

  def dataidFor(printData: Bytes, print: Long, source: Source): Long = inStoreContext {
    if (metaBackend hasSizeAndPrint (source.size, print))
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
    else
      storeSourceData (printData, print, source.allData, source.size)
  }

  protected def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: Source): Long = Memory.reserved(source.size) {
    case Memory.Reserved    (_) => preloadDataThatMayBeAlreadyKnown(printData, print, source)
    case Memory.NotAvailable(_) => source match {
      case source: ResettableSource => readMaybeKnownDataTwiceIfNecessary (printData, print, source)
      case _ => storeSourceData (printData, print, source.allData, source.size)
    }
  }

  protected def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: Source): Long = {
    val bytes = (printData :: source.allData.toList).to[mutable.MutableList]
    val (hash, size) = Hash calculate (hashAlgorithm, bytes iterator)
    metaBackend.dataEntriesFor(size, print, hash).headOption.map(_.id)
      .getOrElse(storeDataFullyPreloaded(bytes, size, print, hash))
  }

  protected def storeDataFullyPreloaded(bytes: mutable.MutableList[Bytes], size: Long, print: Long, hash: Array[Byte]): Long =
    storeSourceData (Bytes consumingIterator bytes, size, print, hash)

  protected def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Long, source: ResettableSource): Long = {
    val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(hashAlgorithm, bytes)
    metaBackend.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source.reset
      storeSourceData(source)
    }
  }

  protected def storeSourceData(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    storeSourceData(printData, Print(printData), source.allData, source.size)
  }

  protected def storeSourceData(printData: Bytes, print: Long, data: Iterator[Bytes], estimatedSize: Long): Long = {
    val (hash, size, dataid) = Hash calculate(hashAlgorithm, Iterator(printData) ++ data, { data =>
      val packedData = StoreMethod.storeCoder(storeMethod)(data)
      storePackedDataAndCreateByteStoreEntries(packedData, estimatedSize)
    })
    metaBackend createDataTableEntry (dataid, size, print, hash, storeMethod)
    dataid
  }

  protected def storeSourceData(data: Iterator[Bytes], size: Long, print: Long, hash: Array[Byte]): Long = {
    val packedData = StoreMethod.storeCoder(storeMethod)(data)
    init(storePackedDataAndCreateByteStoreEntries(data, size)) { dataid =>
      metaBackend createDataTableEntry(dataid, size, print, hash, storeMethod)
    }
  }

  protected def storePackedDataAndCreateByteStoreEntries(data: Iterator[Bytes], estimatedSize: Long): Long = {
    // first store, so if something bad happens while storing no table entries are created
    val storedRanges = normalize(storePackedData(data, estimatedSize))
    init(metaBackend nextDataID) { dataID =>
      storedRanges foreach { case (start, fin) => metaBackend createByteStoreEntry (dataID, start, fin) }
    }
  }

  // Note: storePackedData splits ranges at block borders. In the byte store table, these ranges should be stored contiguously
  protected def normalize(ranges: List[(Long, Long)]): List[(Long, Long)] = ranges.foldLeft(List.empty[(Long, Long)]) {
    case (Nil, range) => List(range)
    case ((headStart, headFin) :: tail, (rangeStart, rangeFin)) if headFin == rangeStart => (headStart, rangeFin) :: tail
    case (ranges, range) => range :: ranges
  }.reverse

  protected sealed trait RangesOrUnstored
  protected final case class Ranges(stored: List[(Long, Long)], remaining: List[(Long, Long)]) extends RangesOrUnstored
  protected final case class Unstored(unstored: List[Bytes]) extends RangesOrUnstored

  protected def storePackedData(data: Iterator[Bytes], estimatedSize: Long): List[(Long, Long)] = {
    // FIXME store everything that fits with estimated size, then continue chunk by chunk
    ???
  }
//    val storeRanges = freeRanges dequeueAtLeast estimatedSize
//    val protocol = data.foldLeft[RangesOrUnstored](Ranges(Nil, storeRanges)) {
//      case (ranges, Bytes(_, _, 0)) => ranges
//      case (ranges, bytes) => storeOneChunk(bytes, ranges)
//    }
//    protocol match {
//      case Unstored(additionalData) =>
//        storeRanges ::: storePackedData(additionalData.iterator, additionalData.sizeInBytes)
//      case Ranges(stored, remaining) =>
//        remaining foreach freeRanges.enqueue
//        stored.reverse
//    }
//  }
//
//  @annotation.tailrec
//  protected final def storeOneChunk(bytes: Bytes, ranges: RangesOrUnstored): RangesOrUnstored = ranges match {
//    case Unstored(data) => Unstored(bytes :: data)
//    case Ranges(_, Nil) => Unstored(List(bytes))
//    case Ranges(stored, head :: tail) =>
//      head.size - bytes.size match {
//        case Size.Negative(_) =>
//          dataBackend.write(bytes withSize head.size, head.start)
//          storeOneChunk(bytes withOffset head.size, Ranges(head :: stored, tail))
//        case Size.Zero =>
//          dataBackend.write(bytes, head.start)
//          Ranges(head :: stored, tail)
//        case _ =>
//          dataBackend.write(bytes, head.start)
//          Ranges((head withLength bytes.size) :: stored, (head withOffset bytes.size) :: tail)
//      }
//    }
//  }

}
