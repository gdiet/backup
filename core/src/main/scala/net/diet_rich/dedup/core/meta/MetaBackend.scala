package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.core.StartFin
import net.diet_rich.dedup.core.data.Print
import net.diet_rich.dedup.util.{Logging, someNow}

trait MetaBackend extends AutoCloseable with Logging {
  def entry(id: Long): Option[TreeEntry]
  def children(parent: Long): List[TreeEntry]
  def children(parent: Long, name: String): List[TreeEntry]
  def entries(path: String): List[TreeEntry]

  def createUnchecked(parent: Long, name: String, changed: Option[Long] = someNow, dataid: Option[Long] = None): Long
  def create(parent: Long, name: String, changed: Option[Long] = someNow, dataid: Option[Long] = None): Long
  def createWithPath(path: String, changed: Option[Long] = someNow, dataid: Option[Long] = None): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long] = someNow, dataid: Option[Long] = None): Long
  def change(id: Long, newParent: Long, newName: String, newChanged: Option[Long], newData: Option[Long], newDeletionTime: Option[Long] = None): Boolean
  def markDeleted(id: Long, deletionTime: Option[Long] = someNow): Boolean

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]
  def createDataTableEntry(reservedid: Long, size: Long, print: Print, hash: Array[Byte], storeMethod: Int): Unit
  def nextDataid: Long

  def dataEntryExists(print: Print): Boolean
  def dataEntryExists(size: Long, print: Print): Boolean
  def dataEntriesFor(size: Long, print: Print, hash: Array[Byte]): List[DataEntry]

  def storeEntries(dataid: Long): List[StartFin]
  def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit

  def settings: String Map String
  def replaceSettings(newSettings: String Map String): Unit

  def inTransaction[T](f: => T): T
  def close(): Unit

  final def path(id: Long): Option[String] =
    if (id == rootEntry.id) Some(rootPath)
    else entry(id) flatMap {entry => path(entry.parent) map (_ + "/" + entry.name)}

  private def firstChild(parent: Long, name: String, children: List[TreeEntry]): Option[TreeEntry] = {
    if (children.size > 1)
      log warn s"Tree entry $parent has multiple children with name $name - taking the first one."
    children.headOption
  }

  final def childWarn (parent: Long, name: String): Option[TreeEntry] =
    firstChild(parent, name, children(parent, name))

  final def childrenWarn(parent: Long): List[TreeEntry] =
    children(parent) groupBy (_ name) flatMap { case (name, children) => firstChild(parent, name, children) } toList
}
