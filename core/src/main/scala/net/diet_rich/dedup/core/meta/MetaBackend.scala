package net.diet_rich.dedup.core.meta

import net.diet_rich.dedup.core.StartFin
import net.diet_rich.dedup.util.now

trait MetaBackend extends AutoCloseable {
  def entry(id: Long): Option[TreeEntry]
  def children(parent: Long): List[TreeEntry]
  def children(parent: Long, name: String): List[TreeEntry]
  def entries(path: String): List[TreeEntry]

  def createUnchecked(parent: Long, name: String, changed: Option[Long] = Some(now), dataid: Option[Long] = None): Long
  def create(parent: Long, name: String, changed: Option[Long] = Some(now), dataid: Option[Long] = None): Long
  def createWithPath(path: String, changed: Option[Long] = Some(now), dataid: Option[Long] = None): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long] = Some(now), dataid: Option[Long] = None): Long
  def change(id: Long, newParent: Long, newName: String, newChanged: Option[Long], newData: Option[Long], newDeletionTime: Option[Long] = None): Boolean
  def markDeleted(id: Long, deletionTime: Option[Long] = Some(now)): Boolean

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]
  def createDataTableEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit
  def nextDataid: Long

  def hasSizeAndPrint(size: Long, print: Long): Boolean
  def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): List[DataEntry]

  def storeEntries(dataid: Long): List[StartFin]
  def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit

  def settings: String Map String
  def replaceSettings(newSettings: String Map String): Unit

  def inTransaction[T](f: => T): T
  def close(): Unit

  final def path(id: Long): Option[String] =
    if (id == rootEntry.id) Some(rootPath)
    else entry(id) flatMap {entry => path(entry.parent) map (_ + "/" + entry.name)}
}