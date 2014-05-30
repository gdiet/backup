// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.util.concurrent._

import net.diet_rich.dedup.core.FileSystem._
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util._

trait FileSystemStoreLogic { _: FileSystemTree =>

  protected val data: FileSystemData
  protected val storeSettings: StoreSettings
  import data._
  import storeSettings._

  private val executorQueue = new ArrayBlockingQueue[Runnable](threadPoolSize)
  private val rejectHandler = new RejectedExecutionHandler {
    override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = executorQueue put r
  }
  private val threadPool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, executorQueue, rejectHandler)
  private val storeContext = scala.concurrent.ExecutionContext fromExecutorService threadPool
  private def inStoreContext[T] (f: => T): T = resultOf(scala.concurrent.Future(f)(storeContext))

  def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntryID = inStoreContext {
    createUnchecked(parent, name, Some(time), Some(dataEntry(source)))
  }

  private[core] def dataEntry(source: Source): DataEntryID = {
    val (print, printData) = printFromSource(source)
    if (hasSizeAndPrint(source size, print))
      tryPreloadToGetDataEntry(printData, print, source)
    else
      storeData(printData, print, source)
  }

  private[core] def tryPreloadToGetDataEntry(printData: Bytes, print: Print, source: Source): DataEntryID = Memory.reserved(source.size.value) {
    case _: MemoryReserved => preloadToGetDataEntry(printData, print, source)
    case _: MemoryNotAvailable => readTwiceIfNecessaryToGetDataEntry(printData, print, source)
  }

  private[core] def readTwiceIfNecessaryToGetDataEntry(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(hashAlgorithm, data)
    dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source.reset
      val (print, printData) = printFromSource(source)
      storeData(printData, print, source)
    }
  }

  private[core] def preloadToGetDataEntry(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: List[Bytes] = printData :: source.allData.toList
    val (hash, size) = Hash.calculate(hashAlgorithm, data.iterator)
    dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse storeData(data, size, print, hash)
  }

  private[core] def storeData(data: List[Bytes], size: Size, print: Print, hash: Hash): DataEntryID =
    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
      case ExistingEntryMatches(dataid) => dataid
      case DataEntryCreated(dataid) =>
        val rangesStored = storeMethod.pack(data.iterator) flatMap { storeBytes(_).reverse }
        rangesStored foreach (createByteStoreEntry(dataid, _))
        dataid
    }

  private[core] def storeData(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size, rangesStored) = Hash.calculate(hashAlgorithm, data, storeMethod pack _ flatMap { storeBytes(_).reverse } toList)
    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
      case ExistingEntryMatches(dataid) =>
        rangesStored foreach requeueFreeRange
        dataid
      case DataEntryCreated(dataid) =>
        rangesStored.reverse foreach (createByteStoreEntry(dataid, _))
        dataid
    }
  }

  // FIXME test separately
  @annotation.tailrec
  private[core] final def storeBytes(bytes: Bytes, offset: Position = Position(0), acc: List[DataRange] = Nil): List[DataRange] = {
    val remainingSize = Position(bytes.length) - offset
    val (block, rest) = nextFreeRange partitionAtSize remainingSize
    writeData(bytes, offset, block)
    if (block.size < remainingSize) {
      assume (rest isEmpty) // FIXME remove when properly tested?
      storeBytes(bytes, offset + block.size, block :: acc)
    } else {
      rest foreach requeueFreeRange
      block :: acc
    }
  }

  private def printFromSource(source: Source): (Print, Bytes) = {
    val printData = source read PRINTSIZE
    (Print(printData), printData)
  }

}
