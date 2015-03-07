package net.diet_rich.dedup.core

import scala.collection.mutable
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.meta.{FreeRanges, MetaBackend}
import net.diet_rich.dedup.util._

trait StoreLogicBackend extends AutoCloseable {
  def dataidFor(source: Source): Long
  def dataidFor(printData: Bytes, print: Long, source: Source): Long
  def close(): Unit
}

class StoreLogic(metaBackend: MetaBackend, writeData: (Bytes, Long) => Unit, freeRanges: FreeRanges,
                 hashAlgorithm: String, storeMethod: Int, val parallel: Int) extends StoreLogicBackend {
  private val internalStoreLogic = new InternalStoreLogic(metaBackend, writeData, freeRanges, hashAlgorithm, storeMethod)
  private val executor = ThreadExecutors.blockingThreadPoolExecutor(parallel)
  private val executionContext = ExecutionContext fromExecutorService executor
  private def resultOf[T] (f: => T): T = Await result (Future(f)(executionContext), 1 day)

  override def dataidFor(source: Source): Long = resultOf { internalStoreLogic dataidFor source }
  override def dataidFor(printData: Bytes, print: Long, source: Source): Long = resultOf { internalStoreLogic dataidFor (printData, print, source) }
  override def close(): Unit = executor close()
}

class InternalStoreLogic(val metaBackend: MetaBackend, val writeData: (Bytes, Long) => Unit,
                         val freeRanges: FreeRanges, val hashAlgorithm: String,
                         val storeMethod: Int) extends StoreLogicDataChecks with StorePackedDataLogic

trait StoreLogicDataChecks {
  val metaBackend: MetaBackend
  val hashAlgorithm: String
  val storeMethod: Int

  def dataidFor(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    dataidFor(printData, Print(printData), source)
  }

  def dataidFor(printData: Bytes, print: Long, source: Source): Long = {
    if (metaBackend hasSizeAndPrint (source.size, print))
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
    else
      storeSourceData (printData, print, source.allData, source.size)
  }

  protected def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: Source): Long = Memory.reserved(source.size * 105 / 100) {
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
      .getOrElse(storeSourceData (Bytes consumingIterator bytes, size, print, hash))
  }

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
    init(metaBackend nextDataid) { dataid =>
      storedRanges foreach { case (start, fin) =>
        assert (fin > start, s"$start - $fin")
        metaBackend createByteStoreEntry (dataid, start, fin) }
    }
  }

  // Note: storePackedData splits ranges at block borders. In the byte store table, these ranges should be stored contiguously
  // FIXME operate on Vector
  protected def normalize(ranges: Ranges): Ranges = ranges.foldLeft(List[StartFin]()) {
    case (Nil, range) => List(range)
    case ((headStart, headFin) :: tail, (rangeStart, rangeFin)) if headFin == rangeStart => (headStart, rangeFin) :: tail
    case (results, range) => range :: results
  }.reverse.toVector

  protected def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges
}

trait StorePackedDataLogic {
  protected def writeData: (Bytes, Long) => Unit
  protected def freeRanges: FreeRanges

  protected def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges = {
    // FIXME use functional style???
    // FIXME normalize already here???
    val storeRanges = mutable.ArrayStack[StartFin]()
    @annotation.tailrec
    def write(protocol: Ranges)(bytes: Bytes): Ranges = {
      assert(bytes.length > 0)
      if (storeRanges.isEmpty) storeRanges.push(freeRanges.nextBlock)
      val (start, fin) = storeRanges pop()
      assert(fin - start <= Int.MaxValue, s"range too large: $start .. $fin")
      val length = (fin - start).toInt
      if (length < bytes.length) {
        writeData (bytes copy (length = length), start)
        write(protocol :+ (start, start + length))(bytes addOffset length)
      } else {
        writeData (bytes, start)
        if (length > bytes.length) storeRanges push ((start + bytes.length, fin))
        protocol :+ (start, start + bytes.length)
      }
    }
    valueOf(data flatMap write(RangesNil) toVector) before (freeRanges pushBack storeRanges)
  }
}
