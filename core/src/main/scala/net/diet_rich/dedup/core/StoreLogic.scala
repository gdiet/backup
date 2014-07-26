// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.util.concurrent.TimeUnit.DAYS

import scala.collection.mutable.MutableList
import scala.concurrent.{ExecutionContext, Future}

import net.diet_rich.dedup.core.values.{TreeEntryID, TreeEntry, Bytes, DataEntryID, Time, Print, Hash, Size}
import net.diet_rich.dedup.util.{Equal, BlockingThreadPoolExecutor, Memory, resultOf}

trait StoreInterface {
  def read(entry: DataEntryID): Iterator[Bytes]
  def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntry
  def createOrReplace(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntry
}

trait StoreLogic extends StoreInterface with Lifecycle { _: TreeInterface with StoreSettingsSlice with DataHandlerSlice =>

  override abstract def teardown() = {
    super.teardown()
    storeExecutor shutdown()
    storeExecutor awaitTermination(1, DAYS)
  }

  override final def read(entry: DataEntryID): Iterator[Bytes] = dataHandler readData entry

  override final def storeUnchecked(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntry = inStoreContext {
    val dataID = dataEntryFor(source)
    createUnchecked(parent, name, Some(time), Some(dataID))
  }

  override final def createOrReplace(parent: TreeEntryID, name: String, source: Source, time: Time): TreeEntry = inStoreContext {
    val dataID = dataEntryFor(source)
    createOrReplace(parent, name, Some(time), Some(dataID))
  }

  private val storeExecutor = BlockingThreadPoolExecutor(storeSettings.threadPoolSize)
  private val storeContext: ExecutionContext =
    ExecutionContext fromExecutorService storeExecutor

  private def inStoreContext[T] (f: => T): T = resultOf(Future(f)(storeContext))

  private def dataEntryFor(source: Source): DataEntryID = {
    val printData = source read FileSystem.PRINTSIZE
    val print = Print(printData)
    if (printData.size === Size(0))
      dataHandler storePackedData(Iterator(), Size(0), print, Hash.calculate(storeSettings hashAlgorithm, Iterator())._1)
    else if (dataHandler hasSizeAndPrint (source size, print))
      tryPreloadDataThatMayBeAlreadyKnown(printData, print, source)
    else
      dataHandler storeSourceData (printData, print, source.allData, source.size)
  }

  import Memory._
  private def tryPreloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = Memory.reserved(source.size.value) {
    case Reserved(_) => preloadDataThatMayBeAlreadyKnown(printData, print, source)
    case NotAvailable(_) => source match {
      case source: ResettableSource => readMaybeKnownDataTwiceIfNecessary (printData, print, source)
      case source => dataHandler storeSourceData (printData, print, source.allData, source.size)
    }
  }

  private def preloadDataThatMayBeAlreadyKnown(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val bytes = (printData :: source.allData.toList).to[MutableList]
    val (hash, size) = Hash calculate (storeSettings hashAlgorithm, bytes iterator)
    dataHandler.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse storeDataFullyPreloaded(bytes, size, print, hash)
  }

  private def storeDataFullyPreloaded(bytes: MutableList[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
    // TODO why don't we call storeSourceData here and let it do the packing?
    val packedData = storeSettings.storeMethod.pack(Bytes.consumingIterator(bytes)).to[MutableList] // FIXME manual test that memory consumption is OK
    dataHandler storePackedData (Bytes consumingIterator packedData, size, print, hash)
  }

  private def readMaybeKnownDataTwiceIfNecessary(printData: Bytes, print: Print, source: ResettableSource): DataEntryID = {
    val bytes: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(storeSettings hashAlgorithm, bytes)
    dataHandler.dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source.reset
      dataHandler storeSourceData source
    }
  }
}
