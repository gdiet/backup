package net.diet_rich.dedup.core

import scala.collection.mutable
import scala.concurrent.{Future, ExecutionContext}

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.meta.{FreeRanges, MetaBackend}
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.Memory.{Reserved, NotAvailable}

trait StoreLogicBackend extends AutoCloseable {
  def dataidFor(printData: Bytes, print: Print, source: Source): Long
  def dataidFor(source: Source): Long
  def futureDataidFor(source: Source): Future[Long]
  def close(): Unit
}

class StoreLogic(metaBackend: MetaBackend, writeData: (Bytes, Long) => Unit, freeRanges: FreeRanges,
                 hashAlgorithm: String, storeMethod: Int, parallel: Int) extends StoreLogicBackend {
  private val internalStoreLogic = new InternalStoreLogic(metaBackend, writeData, freeRanges, hashAlgorithm, storeMethod)
  private val executor = ThreadExecutors.blockingThreadPoolExecutor(parallel)
  private implicit val executionContext = ExecutionContext fromExecutorService executor

  override def dataidFor(printData: Bytes, print: Print, source: Source): Long = resultOf(Future(internalStoreLogic dataidFor (printData, print, source)))
  override def dataidFor(source: Source): Long = resultOf(futureDataidFor(source))
  override def futureDataidFor(source: Source): Future[Long] = Future(internalStoreLogic dataidFor source)
  override def close(): Unit = executor close()
}

class InternalStoreLogic(val metaBackend: MetaBackend, val writeData: (Bytes, Long) => Unit,
                         val freeRanges: FreeRanges, val hashAlgorithm: String,
                         val storeMethod: Int) extends StoreLogicDataChecks with StorePackedDataLogic

trait StoreLogicDataChecks {
  val metaBackend: MetaBackend
  val hashAlgorithm: String
  val storeMethod: Int
  def freeRanges: FreeRanges

  def dataidFor(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    dataidFor(printData, Print(printData), source)
  }

  def dataidFor(printData: Bytes, print: Print, source: Source): Long = source match {
    case sized: SizedSource =>
      if (metaBackend dataEntryExists(sized size, print))
        tryPreloadSizedDataThatMayBeAlreadyKnown(printData, print, sized)
      else
        storeSourceData (printData, print, source.allData)
    case _ =>
      if (metaBackend dataEntryExists print)
        tryPreLoadDataThatMayBeAlreadyKnown(Vector(printData), print, source, 0L)
      else
        storeSourceData (printData, print, source.allData)
  }

  @annotation.tailrec
  final def tryPreLoadDataThatMayBeAlreadyKnown(data: Vector[Bytes], print: Print, source: Source, reserved: Long): Long = {
    val read = try source read 0x100000 catch { case e: Throwable => Memory free reserved; throw e }
    if (read.length == 0) try preloadAndStore(data, print) finally Memory free reserved
    else Memory reserve read.length match {
      case NotAvailable(_) => try storeSourceData(print, (data:+read).iterator ++ source.allData) finally Memory free reserved
      case Reserved(mem) => tryPreLoadDataThatMayBeAlreadyKnown(data :+ read, print, source, reserved + mem)
    }
  }

  def tryPreloadSizedDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: SizedSource): Long = Memory.reserved(source.size * 105 / 100) {
    case Memory.Reserved    (_) => preloadDataThatMayBeAlreadyKnown(printData, print, source)
    case Memory.NotAvailable(_) => source match {
      case source: FileLikeSource => readMaybeKnownDataTwiceIfNecessary (printData, print, source)
      case _ => storeSourceData (printData, print, source.allData)
    }
  }

  def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): Long =
    preloadAndStore(Iterator(printData) ++ source.allData, print)

  def preloadAndStore(data: TraversableOnce[Bytes], print: Print): Long = {
    val bytes = data.to[mutable.MutableList]
    val (hash, size) = Hash calculate (hashAlgorithm, bytes iterator)
    metaBackend.dataEntriesFor(size, print, hash).headOption.map(_.id)
      .getOrElse(storeSourceData (Bytes consumingIterator bytes, size, print, hash))
  }

  def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Print, source: FileLikeSource): Long = {
    val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(hashAlgorithm, bytes)
    metaBackend.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source reset()
      storeSourceData(source)
    }
  }

  def storeSourceData(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    storeSourceData(printData, Print(printData), source.allData)
  }

  def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes]): Long =
    storeSourceData(print, Iterator(printData) ++ data)

  def storeSourceData(print: Print, data: Iterator[Bytes]): Long = {
    val (hash, size, storedRanges) = Hash calculate(hashAlgorithm, data, writeSourceData)
    finishStoringData(print, hash, size, storedRanges)
  }

  def finishStoringData(print: Print, hash: Array[Byte], size: Long, storedRanges: Ranges): Long = {
    metaBackend inTransaction {
      metaBackend dataEntriesFor(size, print, hash) match {
        case Nil =>
          init(metaBackend nextDataid) { dataid =>
            storedRanges foreach { case (start, fin) =>
              assert(fin > start, s"$start - $fin")
              metaBackend createByteStoreEntry(dataid, start, fin)
            }
            metaBackend createDataTableEntry(dataid, size, print, hash, storeMethod)
          }
        case dataEntry :: rest =>
          storedRanges foreach freeRanges.pushBack
          dataEntry.id
      }
    }
  }

  def storeSourceData(data: Iterator[Bytes], size: Long, print: Print, hash: Array[Byte]): Long =
    finishStoringData(print, hash, size, writeSourceData(data))

  def writeSourceData(data: Iterator[Bytes]): Ranges =
    storePackedData(StoreMethod.storeCoder(storeMethod)(data))

  def storePackedData(data: Iterator[Bytes]): Ranges
}

trait StorePackedDataLogic {
  def writeData: (Bytes, Long) => Unit
  def freeRanges: FreeRanges

  def storePackedData(data: Iterator[Bytes]): Ranges = {
    val (finalProtocol, remaining) = data.foldLeft((RangesNil, Option.empty[StartFin])) { case ((protocol, range), bytes) =>
      assert (bytes.length > 0)
      write(bytes, protocol, range getOrElse freeRanges.nextBlock)
    }
    remaining foreach freeRanges.pushBack
    finalProtocol
  }

  @annotation.tailrec
  final def write(bytes: Bytes, protocol: Ranges, free: StartFin): (Ranges, Option[StartFin]) = {
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

  def normalizedAdd(protocol: Ranges, start: Long, fin: Long): Ranges =
    protocol match {
      case heads :+ ((lastStart, `start`)) => heads :+ (lastStart, fin)
      case _ => protocol :+ (start, fin)
    }
}
