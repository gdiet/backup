// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

trait BackupFileSystem extends TreeDB with DataInfoDB with ByteStoreDB with Digesters

object StubbedFileSystem extends BackupFileSystem {
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID = ???
  def fullDataInformation(id: TreeEntryID): Option[net.diet_rich.dedup.database.FullDataInformation] = ???
  def setData(id: TreeEntryID, time: Time, dataid: DataEntryID): Unit = ???
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID] = ???
  
  def calculatePrintAndReset(reader: SeekReader): Print = ???
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Print, ReturnType) = ???
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Hash, ReturnType) = ???
  
  def hasMatch(size: Size, print: Print): Boolean = ???
  def findMatch(size: Size, print: Print, hash: Hash): Option[DataEntryID] = ???
  def createDataEntry(dataid: DataEntryID, size: Size, print: Print, hash: Hash): Unit = ???
  
  def storeAndGetDataId(bytes: Array[Byte], size: Size): DataEntryID = ???
  def storeAndGetDataIdAndSize(reader: Reader): (DataEntryID, Size) = ???
}

case class FullDataInformation (
  time: Time,
  size: Size,
  print: Print,
  hash: Array[Byte],
  dataid: DataEntryID
)

trait Digesters {
  def calculatePrintAndReset(reader: SeekReader): Print
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Print, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Hash, ReturnType)
}

trait TreeDB {
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: TreeEntryID): Option[FullDataInformation]
  /** @throws Exception if the node was not updated correctly. */
  def setData(id: TreeEntryID, time: Time, dataid: DataEntryID): Unit
  /** @return The child's entry ID if any. */
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID]
}

trait DataInfoDB {
  // FIXME
  /** @return true if at least one matching data entry is stored. */
  def hasMatch(size: Size, print: Print): Boolean
  /** @return The data id if a matching data entry is stored. */
  def findMatch(size: Size, print: Print, hash: Hash): Option[DataEntryID]
  /** @throws Exception if the entry was not created correctly. */
  def createDataEntry(dataid: DataEntryID, size: Size, print: Print, hash: Hash): Unit
}

trait ByteStoreDB {
  def storeAndGetDataId(bytes: Array[Byte], size: Size): DataEntryID
  def storeAndGetDataIdAndSize(reader: Reader): (DataEntryID, Size)
}

trait SignatureCalculation {
  def calculatePrintAndReset(reader: SeekReader): Long
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
}