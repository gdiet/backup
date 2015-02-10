package net.diet_rich.dedup.core.tree.sql

import net.diet_rich.dedup.core.tree.{DataEntry, TreeEntry}

import scala.slick.jdbc.StaticQuery

object Tables {
//  // TreeEntries
//  def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id).firstOption
//  def treeChildren(parent: TreeEntryID): List[TreeEntry] = treeChildrenForParentQuery(parent).list
//  def treeChildrenNotDeleted(parent: TreeEntryID): List[TreeEntry] = treeChildrenNotDeletedForParentQuery(parent).list
//  def createTreeEntry(parent: TreeEntryID, name: String, changed: Option[Time], dataid: Option[DataEntryID]): TreeEntry = inTransaction {
//    val id = nextTreeEntryIdQuery.first
//    createTreeEntryUpdate(id, parent, name, changed, dataid).execute
//    TreeEntry(id, parent, name, changed, dataid, None)
//  }
//  def markDeleted(id: TreeEntryID, deletionTime: Option[Time]): Boolean = inTransaction { setTreeEntryDeletedUpdate(deletionTime, id).first === 1 }
//  def updateTreeEntry(id: TreeEntryID, newParent: TreeEntryID, newName: String, newTime: Option[Time], newData: Option[DataEntryID], newDeleted: Option[Time]): Option[TreeEntry] = inTransaction {
//    if (updateTreeEntryUpdate(newParent, newName, newTime, newData, newDeleted, id).first === 1)
//      Some(TreeEntry(id, newParent, newName, newTime, newData, newDeleted))
//    else None
//  }
//
//  // DataEntries
//  def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id).firstOption
//  def dataEntries(size: Size, print: Print): List[DataEntry] = dataEntriesForSizePrintQuery(size, print).list
//  def dataEntries(size: Size, print: Print, hash: Hash): List[DataEntry] = dataEntriesForSizePrintHashQuery(size, print, hash).list
//  def createDataEntry(reservedID: DataEntryID, size: Size, print: Print, hash: Hash, method: StoreMethod): Unit = inTransaction { createDataEntryUpdate(reservedID, size, print, hash, method).execute }
//  def nextDataID: DataEntryID = nextDataEntryIdQuery.first
//
//  // ByteStore
//  def storeEntries(id: DataEntryID): List[StoreEntry] = storeEntriesForIdQuery(id).list
  def createByteStoreEntry(dataid: Long, start: Long, fin: Long)(implicit session: CurrentSession): Unit = inTransaction { createStoreEntryUpdate(dataid, start, fin).execute }

//  // basic select statements
//  val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
//  val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
//  val selectFromByteStore = "SELECT id, dataid, start, fin FROM ByteStore"
//
//  // TreeEntries
//  val sortedTreeEntriesQuery = StaticQuery.queryNA[TreeEntry](s"$selectFromTreeEntries ORDER BY id;")
//  val treeEntryForIdQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
//  val treeChildrenForParentQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
//  val treeChildrenNotDeletedForParentQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE parent = ? and deleted is NULL;")
//  val nextTreeEntryIdQuery = StaticQuery.queryNA[Long]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
//  val setTreeEntryDeletedUpdate = StaticQuery.update[(Option[Long], Long)]("UPDATE TreeEntries SET deleted = ? WHERE id = ?;")
//  val updateTreeEntryUpdate = StaticQuery.update[(Long, String, Option[Long], Option[Long], Option[Long], Long)]("UPDATE TreeEntries SET parent = ?, name = ?, changed = ?, dataid = ?, deleted = ? WHERE id = ? AND deleted IS NULL;")
//  val createTreeEntryUpdate = StaticQuery.update[(Long, Long, String, Option[Long], Option[Long])]("INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES (?, ?, ?, ?, ?);")
//
//  // DataEntries
//  val dataEntryForIdQuery = StaticQuery.query[Long, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
//  val dataEntriesForSizePrintQuery = StaticQuery.query[(Long, Long), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
//  val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Long, Long, Array[Byte]), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
//  val nextDataEntryIdQuery = StaticQuery.queryNA[Long]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")
//  val createDataEntryUpdate = StaticQuery.update[(Long, Long, Long, Array[Byte], Int)]("INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")
//
//  // ByteStore
//  val storeEntriesForIdQuery = StaticQuery.query[Long, StoreEntry](s"$selectFromByteStore WHERE dataid = ? ORDER BY id ASC;")
  val createStoreEntryUpdate = StaticQuery.update[(Long, Long, Long)]("INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?);")

  // Note: Writing is synchronized, so "create only if not exists" can be implemented.
  def inTransaction[T](f: => T): T = synchronized(f)
}
