// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

// FIXME check imports
import scala.collection.mutable.MutableList
import scala.concurrent.{ExecutionContext, Future}

import net.diet_rich.dedup.core.FileSystem._
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util._

trait StoreLogic extends StoreInterface { _: StoreSettingsSlice with TreeInterface with DataHandlerSlice =>

  override final def read(entry: DataEntryID): Iterator[Bytes] = data readData entry

  override final def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntryID = inStoreContext {
    val dataID = dataEntryFor(source)
    createUnchecked(parent, name, Some(time), Some(dataID))
  }

  private val storeContext = {
    import java.util.concurrent._
    import storeSettings._
    val executorQueue = new ArrayBlockingQueue[Runnable](threadPoolSize)
    val rejectHandler = new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = executorQueue put r
    }
    val threadPool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, executorQueue, rejectHandler)
    ExecutionContext fromExecutorService threadPool
  }

  private def inStoreContext[T] (f: => T): T = resultOf(Future(f)(storeContext))

  protected def dataEntryFor(source: Source): DataEntryID = {
    val printData = source read PRINTSIZE
    val print = Print(printData)
    if (data hasSizeAndPrint (source size, print))
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
    else
      storeDataThatIsKnownToBeNew(printData, print, source)
  }

  protected def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = Memory.reserved(source.size.value) {
    case MemoryReserved(_) => preloadDataThatMayBeAlreadyKnown(printData, print, source)
    case MemoryNotAvailable(_) => readMaybeKnownDataTwiceIfNecessary(printData, print, source)
  }

  protected def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val bytes = (printData :: source.allData.toList).to[MutableList] // FIXME manual test that memory consumption is OK
    val (hash, size) = Hash calculate (storeSettings hashAlgorithm, bytes)
    data.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse storeDataFullyPreloaded(bytes, size, print, hash)
  }

  protected def storeDataFullyPreloaded(bytes: MutableList[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
    val packedData = storeSettings.storeMethod.pack(Bytes.consumingIterator(bytes)).toList
    data storeData packedData
  }


  protected def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Print, source: Source): DataEntryID = {
    ???
//    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
//    val (hash, size) = Hash.calculate(hashAlgorithm, data)
//    dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
//      source.reset
//      val (print, printData) = printFromSource(source)
//      storeData(printData, print, source)
//    }
  }

  protected def storeDataThatIsKnownToBeNew(printData: Bytes, print: Print, source: Source): DataEntryID = {
    ???
//    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
//    val (hash, size, rangesStored) = Hash.calculate(hashAlgorithm, data, storeMethod pack _ flatMap { storeBytes(_).reverse } toList)
//    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
//      case ExistingEntryMatches(dataid) =>
//        rangesStored foreach requeueFreeRange
//        dataid
//      case DataEntryCreated(dataid) =>
//        rangesStored foreach (createByteStoreEntry(dataid, _))
//        dataid
//    }
  }

//  @annotation.tailrec
//  protected final def storeBytes(bytes: Bytes, acc: List[DataRange] = Nil): List[DataRange] = {
//    val (block, rest) = nextFreeRange partitionAtLimit bytes.size
//    dataBackend.write(bytes, block)
//    if (block.size < bytes.size) {
//      assume (rest isEmpty)
//      storeBytes(bytes withOffset block.size, block :: acc)
//    } else {
//      rest foreach requeueFreeRange
//      block :: acc
//    }
//  }

}
