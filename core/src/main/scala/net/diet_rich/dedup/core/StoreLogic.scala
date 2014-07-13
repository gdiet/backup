// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.collection.mutable.MutableList
import scala.concurrent.{ExecutionContext, Future}

import net.diet_rich.dedup.core.values.{TreeEntryID, Bytes, DataEntryID, Time, Print, Hash, Size}
import net.diet_rich.dedup.util.{BlockingThreadPoolExecutor, Memory, resultOf}

trait StoreLogic extends StoreInterface { _: TreeInterface with StoreSettingsSlice with DataHandlerSlice =>

  override final def read(entry: DataEntryID): Iterator[Bytes] = dataHandler readData entry

  override final def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntryID = inStoreContext {
    val dataID = dataEntryFor(source)
    createUnchecked(parent, name, Some(time), Some(dataID))
  }

  private val storeContext: ExecutionContext =
    ExecutionContext fromExecutorService BlockingThreadPoolExecutor(storeSettings.threadPoolSize)

  private def inStoreContext[T] (f: => T): T = resultOf(Future(f)(storeContext))

  protected def dataEntryFor(source: Source): DataEntryID = {
    val printData = source read FileSystem.PRINTSIZE
    val print = Print(printData)
    if (dataHandler hasSizeAndPrint (source size, print))
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
    else
      dataHandler storeSourceData (printData, print, source.allData, source.size)
  }

  import Memory._
  protected def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = Memory.reserved(source.size.value) {
    case Reserved(_) => preloadDataThatMayBeAlreadyKnown(printData, print, source)
    case NotAvailable(_) => readMaybeKnownDataTwiceIfNecessary(printData, print, source)
  }

  protected def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val bytes = (printData :: source.allData.toList).to[MutableList]
    val (hash, size) = Hash calculate (storeSettings hashAlgorithm, bytes iterator)
    dataHandler.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse storeDataFullyPreloaded(bytes, size, print, hash)
  }

  protected def storeDataFullyPreloaded(bytes: MutableList[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
    val packedData = storeSettings.storeMethod.pack(Bytes.consumingIterator(bytes)).to[MutableList] // FIXME manual test that memory consumption is OK
    dataHandler storePackedData (Bytes consumingIterator packedData, size, print, hash)
  }

  protected def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(storeSettings hashAlgorithm, bytes)
    dataHandler.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source.reset
      dataHandler storeSourceData source
    }
  }
}
