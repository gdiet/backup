package net.diet_rich.dedupfs

import net.diet_rich.common._, Memory._
import net.diet_rich.common.io._
import net.diet_rich.dedupfs.metadata._

class StoreLogic {

}

object StoreLogic {
  val preloadBlockSize = 0x100000
}

class InternalStoreLogic(metaBackend: Metadata, protected val writeData: (Bytes, Long) => Unit,
                         protected val freeRanges: FreeRanges, hashAlgorithm: String,
                         storeMethod: Int) extends StorePackedDataLogic {
  import StoreLogic._

  def dataidFor(source: Source): Long = {
    val printData = source read Repository.PRINTSIZE
    dataidFor(printData, printOf(printData), source)
  }

  def dataidFor(printData: Bytes, print: Long, source: Source): Long = source match {
    case sized: SizedSource =>
      if (metaBackend dataEntryExists(sized.size, print))
        tryPreloadSizedDataThatMayBeAlreadyKnown(printData, print, sized)
      else
        storeSourceData (printData, print, source.allData)
    case _ =>
      if (metaBackend dataEntryExists print)
        tryPreLoadDataThatMayBeAlreadyKnown(Seq(printData), print, source, 0L)
      else
        storeSourceData (printData, print, source.allData)
  }

  @annotation.tailrec
  private def tryPreLoadDataThatMayBeAlreadyKnown(data: Seq[Bytes], print: Long, source: Source, reserved: Long): Long = {
    Memory reserve preloadBlockSize match {
      case NotAvailable(_) => try storeSourceData(print, data.iterator ++ source.allData) finally Memory free reserved
      case Reserved(_) =>
        val read = try source read preloadBlockSize catch { case e: Throwable => Memory free reserved; throw e }
        if (read.length < preloadBlockSize) Memory free (preloadBlockSize - read.length)
        if (read.length == 0)
          try storePreloadedIfNotKnown(data, print) finally Memory free reserved
        else
          tryPreLoadDataThatMayBeAlreadyKnown(data :+ read, print, source, reserved + read.length)
    }
  }

  private def tryPreloadSizedDataThatMayBeAlreadyKnown(printData: Bytes, print: Long, source: SizedSource): Long = Memory.reserved(source.size * 105 / 100) {
    case Reserved(_) => storePreloadedIfNotKnown(Iterator(printData) ++ source.allData, print)
    case NotAvailable(_) => source match {
      case source: FileLikeSource => readMaybeKnownDataTwiceIfNecessary (printData, print, source)
      case _ => storeSourceData (printData, print, source.allData)
    }
  }

  private def storePreloadedIfNotKnown(data: TraversableOnce[Bytes], print: Long): Long = {
    val bytes = data.toArray
    val (hash, size) = Hash calculate (hashAlgorithm, bytes.iterator)
    metaBackend.dataEntriesFor(size, print, hash).headOption.map(_.id)
      .getOrElse(storeSourceData (Bytes consumingIterator bytes, size, print, hash))
  }

  def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Long, source: FileLikeSource): Long = ??? // {
//    val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
//    val (hash, size) = Hash.calculate(hashAlgorithm, bytes)
//    metaBackend.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
//      source reset()
//      storeSourceData(source)
//    }
//  }
//
//  def storeSourceData(source: Source): Long = {
//    val printData = source read Repository.PRINTSIZE
//    storeSourceData(printData, Print(printData), source.allData)
//  }
//
  def storeSourceData(printData: Bytes, print: Long, data: Iterator[Bytes]): Long = ???
//    storeSourceData(print, Iterator(printData) ++ data)
//
  def storeSourceData(print: Long, data: Iterator[Bytes]): Long = ??? // {
//    val (hash, size, storedRanges) = Hash calculate(hashAlgorithm, data, writeSourceData)
//    finishStoringData(print, hash, size, storedRanges)
//  }
//
//  def finishStoringData(print: Print, hash: Array[Byte], size: Long, storedRanges: Ranges): Long = {
//    metaBackend inTransaction {
//      metaBackend dataEntriesFor(size, print, hash) match {
//        case Nil =>
//          init(metaBackend nextDataid) { dataid =>
//            storedRanges foreach { case (start, fin) =>
//              assert(fin > start, s"$start - $fin")
//              metaBackend createByteStoreEntry(dataid, start, fin)
//            }
//            metaBackend createDataTableEntry(dataid, size, print, hash, storeMethod)
//          }
//        case dataEntry :: rest =>
//          storedRanges foreach freeRanges.pushBack
//          dataEntry.id
//      }
//    }
//  }
//
  def storeSourceData(data: Iterator[Bytes], size: Long, print: Long, hash: Array[Byte]): Long = ???
//    finishStoringData(print, hash, size, writeSourceData(data))
//
//  def writeSourceData(data: Iterator[Bytes]): Ranges =
//    storePackedData(StoreMethod.storeCoder(storeMethod)(data))
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
