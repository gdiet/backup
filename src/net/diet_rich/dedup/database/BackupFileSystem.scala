// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

trait BackupFileSystem extends TreeDB with DataInfoDB with ByteStoreDB

object StubbedFileSystem extends BackupFileSystem {
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID = ???
  def fullDataInformation(id: TreeEntryID): Option[net.diet_rich.dedup.database.FullDataInformation] = ???
  def setData(id: TreeEntryID, time: Long, dataid: Long): Unit = ???
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID] = ???
}

case class FullDataInformation (
  time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  dataid: Long
)

trait TreeDB {
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: TreeEntryID): Option[FullDataInformation]
  /** @throws Exception if the node was not updated correctly. */
  def setData(id: TreeEntryID, time: Long, dataid: Long): Unit
  /** @return The child's entry ID if any. */
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID]
}

trait DataInfoDB {
  // FIXME
//  /** @return true if at least one matching data entry is stored. */
//  def hasMatch(size: Long, print: Long): Boolean
//  /** @return The data id if a matching data entry is stored. */
//  def findMatch(size: Long, print: Long, hash: Array[Byte]): Option[Long]
//  /** @throws Exception if the entry was not created correctly. */
//  def createDataEntry(dataid: Long, size: Long, print: Long, hash: Array[Byte]): Unit
}

trait ByteStoreDB {
  // FIXME
//  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
//  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
}
