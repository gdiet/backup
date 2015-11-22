package net.diet_rich.dedupfs

import scala.concurrent.Future

import net.diet_rich.common._, Executors._, Memory._
import net.diet_rich.common.io._
import net.diet_rich.dedupfs.metadata._

trait StoreLogic extends AutoCloseable {
  def dataidFor(printData: Bytes, print: Print, source: Source): Long
  def dataidFor(source: Source): Long
  def futureDataidFor(source: Source): Future[Long]
  def close(): Unit
}

object StoreLogic {
  val preloadBlockSize = 0x100000
  def apply(metaBackend: Metadata, writeData: (Bytes, Long) => Unit, freeRanges: FreeRanges, hashAlgorithm: String, storeMethod: Int, parallel: Int): StoreLogic =
    new SingleThreadedStoreLogic(metaBackend, writeData, freeRanges, hashAlgorithm, storeMethod) with StoreLogic {
      private implicit val context = blockingThreadPoolExecutionContext(parallel)
      override def futureDataidFor(source: Source): Future[Long] = Future(dataidFor(source))
      override def close(): Unit = { context close() }
    }
}

class SingleThreadedStoreLogic(metaBackend: Metadata, protected val writeData: (Bytes, Long) => Unit,
                         protected val freeRanges: FreeRanges, hashAlgorithm: String,
                         storeMethod: Int) extends StorePackedDataLogic {
  import StoreLogic._

  def dataidFor(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    dataidFor(printData, Print printOf printData, source)
  }

  def dataidFor(printData: Bytes, print: Print, source: Source): Long = source match {
    case sized: SizedSource =>
      if (metaBackend dataEntryExists(sized.size, print))
        tryPreloadSizedDataThatMayBeAlreadyKnown(printData, print, sized)
      else
        storeSourceData(printData +: source.allData, print)
    case _ =>
      if (metaBackend dataEntryExists print)
        tryPreLoadDataThatMayBeAlreadyKnown(Seq(printData), print, source, 0L)
      else
        storeSourceData(printData +: source.allData, print)
  }

  @annotation.tailrec
  private def tryPreLoadDataThatMayBeAlreadyKnown(data: Seq[Bytes], print: Print, source: Source, reserved: Long): Long = {
    Memory reserve preloadBlockSize match {
      case NotAvailable(_) => try storeSourceData(data.iterator ++ source.allData, print) finally Memory free reserved
      case Reserved(_) =>
        val read = try source read preloadBlockSize catch { case e: Throwable => Memory free reserved; throw e }
        if (read.length < preloadBlockSize) Memory free (preloadBlockSize - read.length)
        if (read.length == 0)
          try storePreloadedIfNotKnown(data, print) finally Memory free reserved
        else
          tryPreLoadDataThatMayBeAlreadyKnown(data :+ read, print, source, reserved + read.length)
    }
  }

  private def tryPreloadSizedDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: SizedSource): Long =
    Memory.reserved(source.size * 105 / 100) {
      case Reserved(_) => storePreloadedIfNotKnown(printData +: source.allData, print)
      case NotAvailable(_) => source match {
        case source: FileLikeSource => readMaybeKnownDataTwiceIfNecessary (printData, print, source)
        case _ => storeSourceData(printData +: source.allData, print)
      }
    }

  private def storePreloadedIfNotKnown(data: TraversableOnce[Bytes], print: Print): Long = {
    val bytes = data.toArray
    val (hash, size) = Hash calculate (hashAlgorithm, bytes.iterator)
    metaBackend.dataEntriesFor(size, print, hash).headOption.map(_.id)
      .getOrElse(storeSourceData (Bytes consumingIterator bytes, size, print, hash))
  }

  private def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Print, source: FileLikeSource): Long = {
    val bytes: Iterator[Bytes] = printData +: source.allData
    val (hash, size) = Hash.calculate(hashAlgorithm, bytes)
    metaBackend.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source reset()
      val printData = source read Repository.PRINTSIZE
      storeSourceData(printData +: source.allData, Print printOf printData)
    }
  }

  private def storeSourceData(data: Iterator[Bytes], print: Print): Long = {
    val (hash, size, storedRanges) = Hash calculate(hashAlgorithm, data, writeSourceData)
    finishStoringData(storedRanges, size, print, hash)
  }

  private def storeSourceData(data: Iterator[Bytes], size: Long, print: Print, hash: Array[Byte]): Long =
    finishStoringData(writeSourceData(data), size, print, hash)

  private def writeSourceData(data: Iterator[Bytes]): Ranges =
    storePackedData(StoreMethod.storeCoder(storeMethod)(data))

  private def finishStoringData(storedRanges: Ranges, size: Long, print: Print, hash: Array[Byte]): Long = {
    metaBackend inTransaction {
      metaBackend dataEntriesFor(size, print, hash) match {
        case Seq() =>
          init(metaBackend.nextDataid) { dataid =>
            storedRanges foreach { case (start, fin) =>
              assert(fin > start, s"$start - $fin")
              metaBackend createByteStoreEntry(dataid, start, fin)
            }
            metaBackend createDataEntry(dataid, size, print, hash, storeMethod)
          }
        case dataEntry +: _ =>
          storedRanges foreach freeRanges.pushBack
          dataEntry.id
      }
    }
  }
}


trait StorePackedDataLogic {
  protected def writeData: (Bytes, Long) => Unit
  protected def freeRanges: FreeRanges

  protected def storePackedData(data: Iterator[Bytes]): Ranges = {
    val (finalProtocol, remaining) = data.foldLeft((NoRanges, Option.empty[Range])) { case ((protocol, range), bytes) =>
      assert (bytes.length > 0)
      write(bytes, protocol, range getOrElse freeRanges.nextBlock)
    }
    remaining foreach freeRanges.pushBack
    finalProtocol
  }

  @annotation.tailrec
  private def write(bytes: Bytes, protocol: Ranges, free: Range): (Ranges, Option[Range]) = {
    val (start, fin) = free
    val length = fin - start
    if (length >= bytes.length) {
      writeData (bytes, start)
      val rest = if (length == bytes.length) None else Some((start + bytes.length, fin))
      (normalizedAdd(protocol, start, start + bytes.length), rest)
    } else {
      writeData (bytes copy (length = length.toInt), start)
      write(bytes addOffset length.toInt, normalizedAdd(protocol, start, fin), freeRanges.nextBlock)
    }
  }

  private def normalizedAdd(protocol: Ranges, start: Long, fin: Long): Ranges =
    protocol match {
      case heads :+ ((lastStart, `start`)) => heads :+ (lastStart, fin)
      case _ => protocol :+ (start, fin)
    }
}
