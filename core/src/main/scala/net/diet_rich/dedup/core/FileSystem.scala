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

trait FileSystem extends FileSystemTree with FileSystemStoreLogic with FileSystemData

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

trait FileSystemTree {
  import FileSystem._

  protected val sqlTables: SQLTables

  def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry] = sqlTables treeChildren parent
  def children(parent: TreeEntryID): List[TreeEntry] = childrenWithDeleted(parent) filter (_.deleted isEmpty)

  def createUnchecked(parent: TreeEntryID, name: String, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID =
    sqlTables createTreeEntry (parent, name, time, dataid)
  def create(parent: TreeEntryID, name: String, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, time, dataid)
    }
  }
  def createWithPath(path: Path, time: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntryID = sqlTables inWriteContext {
    val elements = path.elements
    if(elements.size == 0) throw new IOException("can't create the root entry")
    val parent = elements.dropRight(1).foldLeft(ROOTID) { (node, childName) =>
      children(node) filter (_.name == childName) match {
        case Nil => createUnchecked(node, childName, time, dataid)
        case List(entry) => entry.id
        case entries => throw new IOException(s"ambiguous path; §entries")
      }
    }
    create(parent, elements.last, time, dataid)
  }

  def entries(path: Path): List[TreeEntry] =
    path.elements.foldLeft(List(ROOTENTRY)) { (node, childName) =>
      node flatMap (children(_) filter (_.name == childName))
    }
}

trait FileSystemStoreLogic { _: FileSystem =>
  import FileSystem._

  import net.diet_rich.dedup.util.Bytes
  import java.util.concurrent._
  import scala.concurrent.{Future, ExecutionContext}

  protected val storeSettings: StoreSettings
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
    val (hash, size) = Hash.calculate(hashAlgorithm)(data)
    dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse {
      source.reset
      val (print, printData) = printFromSource(source)
      storeData(printData, print, source)
    }
  }

  private[core] def preloadToGetDataEntry(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: List[Bytes] = printData :: source.allData.toList
    val (hash, size) = Hash.calculate(hashAlgorithm)(data.iterator)
    dataEntriesFor(size, print, hash).headOption map (_.id) getOrElse storeData(data, size, print, hash)
  }

  private[core] def storeData(printData: Bytes, print: Print, source: Source): DataEntryID = {
    val data: Iterator[Bytes] = Iterator(printData) ++ source.allData
    val (hash, size) = Hash.calculate(hashAlgorithm)(data)
    ???
  }

  private[core] def storeData(data: List[Bytes], size: Size, print: Print, hash: Hash): DataEntryID =
    createDataEntry(size, print, hash, storeSettings.storeMethod) match {
      case ExistingEntryMatches(dataid) => dataid
      case DataEntryCreated(dataid) =>
        ??? // FIXME implement the actual store process
        dataid
    }

  private def printFromSource(source: Source): (Print, Bytes) = {
    val printData = source read PRINTSIZE
    (Print(printData), printData)
  }

}

sealed trait DataEntryCreateResult { val id: DataEntryID }
case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult

trait FileSystemData {

  protected val sqlTables: SQLTables
  protected val dataSettings: DataSettings
  import dataSettings.blocksize

  private[core] def hasSizeAndPrint(size: Size, print: Print): Boolean = !(sqlTables dataEntries(size, print) isEmpty)
  private[core] def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = sqlTables dataEntries(size, print, hash)
  private[core] def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryCreateResult = sqlTables.inWriteContext {
    sqlTables.dataEntries(size, print, hash).headOption
      .map(e => ExistingEntryMatches(e.id))
      .getOrElse(DataEntryCreated(sqlTables.createDataEntry(size, print, hash, method)))
  }

  private[core] val freeRangesQueue = mutable.Queue[DataRange](DataRange(sqlTables.startOfFreeDataArea, Position(Long MaxValue)))

  // queue gaps in byte store
  if (sqlTables.illegalDataAreaOverlapsValue.isEmpty) {
    val dataAreaStarts = sqlTables.dataAreaStarts
    if (!dataAreaStarts.isEmpty) {
      val (firstArea :: gapStarts) = dataAreaStarts
      if (firstArea > Position(0L)) freeRangesQueue enqueue DataRange(Position(0L), firstArea)
      freeRangesQueue enqueue ((sqlTables.dataAreaEnds zip gapStarts).map(DataRange.tupled):_*)
    }
  }

  private[core] def requestFreeRange: DataRange = synchronized {
    val (range, rest) = freeRangesQueue.dequeue().blockPartition(blocksize)
    rest foreach (freeRangesQueue.enqueue(_))
    range
  }
  private[core] def returnFreeRange (range: DataRange): Unit = synchronized { freeRangesQueue enqueue range }

}
