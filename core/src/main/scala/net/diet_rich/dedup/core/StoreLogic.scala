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
      handleCachedSource(printData, print, source)
    else
      storeSourceData (printData, print, source.allData, source.size)
  }

  // FIXME test
  protected def handleCachedSource(printData: Bytes, print: Long, source: Source): Long = source match {
    case cached: CachedSource =>
      val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
      val (hash, size) = Hash.calculate(hashAlgorithm, bytes)
      assert(size == source.size)
      metaBackend.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
        cached.reset
        storeSourceData(source.allData, size, print, hash)
      }
    case _ =>
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
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
    // first store everything, so if something bad happens while storing no table entries are created
    val storedRanges = storePackedData(data, estimatedSize)
    init(metaBackend nextDataid) { dataid =>
      storedRanges foreach { case (start, fin) =>
        assert (fin > start, s"$start - $fin")
        metaBackend createByteStoreEntry (dataid, start, fin) }
    }
  }

  protected def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges
}

trait StorePackedDataLogic {
  protected def writeData: (Bytes, Long) => Unit
  protected def freeRanges: FreeRanges

  protected def storePackedData(data: Iterator[Bytes], estimatedSize: Long): Ranges = {
    val (finalProtocol, remaining) = data.foldLeft((RangesNil, Option.empty[StartFin])) { case ((protocol, range), bytes) =>
      assert (bytes.length > 0)
      write(bytes, protocol, range getOrElse freeRanges.nextBlock)
    }
    remaining foreach freeRanges.pushBack
    finalProtocol
  }

  @annotation.tailrec
  protected final def write(bytes: Bytes, protocol: Ranges, free: StartFin): (Ranges, Option[StartFin]) = {
    val (start, fin) = free
    assert(fin - start <= Int.MaxValue, s"range too large: $start .. $fin")
    val length = (fin - start).toInt
    if (length >= bytes.length) {
      writeData (bytes, start)
      val rest = if (length == bytes.length) None else Some((start + bytes.length, fin))
      (normalizedAdd(protocol, start, start + bytes.length), rest)
    } else {
      writeData (bytes copy (length = length), start)
      write(bytes addOffset length, normalizedAdd(protocol, start, fin), freeRanges.nextBlock)
    }
  }

  protected def normalizedAdd(protocol: Ranges, start: Long, fin: Long): Ranges =
    protocol match {
      case heads :+ ((lastStart, `start`)) => heads :+ (lastStart, fin)
      case _ => protocol :+ (start, fin)
    }
}