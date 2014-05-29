// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.IOException
import scala.concurrent.Future

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.core.values.Implicits._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.core.values.DataEntry
import net.diet_rich.dedup.core.values.TreeEntry
import scala.Some
import net.diet_rich.dedup.core.values.Size
import net.diet_rich.dedup.util.MemoryReserved
import net.diet_rich.dedup.core.values.TreeEntryID
import scala.collection.mutable

class FileSystem(
  protected val data: FileSystemData,
  protected val storeSettings: StoreSettings,
  protected val sqlTables: SQLTables
) extends SQLTables.Component with FileSystemTree with FileSystemStoreLogic

object FileSystem {
  val ROOTID = TreeEntryID(0)
  val ROOTPARENTID = TreeEntryID(-1)
  val ROOTENTRY = TreeEntry(ROOTID, ROOTPARENTID, Path.ROOTNAME, None, None, None)
  val PRINTSIZE = 8192
}

trait StoreSettings {
  def hashAlgorithm = "MD5"
  def threadPoolSize = 4
  def storeMethod = StoreMethod.DEFLATE
}

trait DataSettings {
  def blocksize: Size = Size(0x800000L)
}

trait FileSystemStoreLogic { _: FileSystemTree =>
  import FileSystem._

  import net.diet_rich.dedup.util.Bytes
  import java.util.concurrent._
  import scala.concurrent.{Future, ExecutionContext}

  protected val data: FileSystemData
  protected val storeSettings: StoreSettings
  import data._
  import storeSettings._

  private val executorQueue = new ArrayBlockingQueue[Runnable](threadPoolSize)
  private val rejectHandler = new RejectedExecutionHandler {
    override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = executorQueue put r
  }
  private val threadPool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, executorQueue, rejectHandler)
  private val storeContext: ExecutionContext = ExecutionContext fromExecutorService threadPool
  private def inStoreContext[T] (f: => T): T = resultOf(Future(f)(storeContext))

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

  private[core] def storeData(data: List[Bytes], size: Size, print: Print, hash: Hash): DataEntryID =
    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
      case ExistingEntryMatches(dataid) => dataid
      case DataEntryCreated(dataid) =>
        val rangesStored = data flatMap { storeBytes(_) }
        rangesStored.reverse foreach (createByteStoreEntry(dataid, _)) // FIXME don't use sqlTables here, make it impossible to use them
        dataid
    }

  private[core] def storeData(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size, rangesStored) = Hash.calculate(hashAlgorithm, data, storeBytes(_).reverse)
    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
      case ExistingEntryMatches(dataid) =>
        rangesStored.flatten foreach requeueFreeRange
        dataid
      case DataEntryCreated(dataid) =>
        rangesStored.flatten foreach (createByteStoreEntry(dataid, _))
        dataid
    }
  }

  private def printFromSource(source: Source): (Print, Bytes) = {
    val printData = source read PRINTSIZE
    (Print(printData), printData)
  }

}

sealed trait DataEntryCreateResult { val id: DataEntryID }
case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult
