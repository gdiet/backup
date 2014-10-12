// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.language.reflectiveCalls

import Runtime.{getRuntime => runtime}

import net.diet_rich.dedup.core.values._

import scala.collection.mutable.MutableList

object ManualMemoryConsumptionTest extends App {
  def max = runtime.maxMemory
  def free = runtime.freeMemory + runtime.maxMemory - runtime.totalMemory
  val totalSize = max / 3 * 2
  val pieceSize = totalSize / 10

  val storeLogic = new StoreLogic with TreeInterface with StoreSettingsSlice with DataHandlerSlice {

    def entry(id: TreeEntryID): Option[TreeEntry] = ???
    def dataEntry(id: DataEntryID): Option[DataEntry] = ???
    def childrenWithDeleted(parent: TreeEntryID): List[TreeEntry] = ???
    def children(parent: TreeEntryID): List[TreeEntry] = ???
    def children(parent: TreeEntryID, name: String): List[TreeEntry] = ???
    def createUnchecked(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = ???
    def create(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = ???
    def createWithPath(path: Path, changed: Option[Time] = Some(Time now), dataid: Option[DataEntryID] = None): TreeEntry = ???
    def markDeleted(id: TreeEntryID, deletionTime: Option[Time] = Some(Time now)): Boolean = ???
    def change(id: TreeEntryID, newParent: TreeEntryID, newName: String, newTime: Option[Time], newData: Option[DataEntryID], newDeletionTime: Option[Time] = None): Option[TreeEntry] = ???
    def createOrReplace(parent: TreeEntryID, name: String, changed: Option[Time] = None, dataid: Option[DataEntryID] = None): TreeEntry = ???
    def sizeOf(id: DataEntryID): Option[Size] = ???
    def entries(path: Path): List[TreeEntry] = ???
    def inTransaction[T](f: => T): T = ???

    def storeSettings: StoreSettings = StoreSettings ("MD5", 4, StoreMethod.STORE)

    def dataHandler: DataHandler = new DataHandler {
      def readData(entry: DataEntryID): Iterator[Bytes] = ???
      def hasSizeAndPrint(size: Size, print: Print): Boolean = ???
      def dataEntriesFor(size: Size, print: Print, hash: Hash): List[DataEntry] = ???
      def storeSourceData(data: Source): DataEntryID = ???
      def storeSourceData(printData: Bytes, print: Print, data: Iterator[Bytes], estimatedSize: Size): DataEntryID = ???
      def storeSourceData(data: Iterator[Bytes], size: Size, print: Print, hash: Hash): DataEntryID = {
        val packedData = storeSettings.storeMethod.pack(data)
        while(packedData.hasNext) {
          packedData.next()
          runtime.gc()
          Thread sleep 300
          println(s"free memory: $free of $max")
        }
        DataEntryID(1)
      }
    }

    def storeTestProbe(bytes: MutableList[Bytes], size: Size): DataEntryID = storeDataFullyPreloaded(bytes, size, Print(0), Hash(Array()))
  }

  println(s"initial free memory: $free of $max")
  storeLogic.storeTestProbe(List.fill(10){ Bytes.zero(pieceSize.toInt) }.to[MutableList], Size(totalSize))
  println(s"final free memory: $free of $max")
}
