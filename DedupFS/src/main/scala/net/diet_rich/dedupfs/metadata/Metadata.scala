package net.diet_rich.dedupfs.metadata

trait Metadata extends AutoCloseable {
  def entry(key: Long): Option[TreeEntry]
  def allChildren(parent: Long): Seq[TreeEntry]
  def children(parent: Long): Seq[TreeEntry]
  def allChildren(parent: Long, name: String): Seq[TreeEntry]
  def child(parent: Long, name: String): Seq[TreeEntry]

  def allEntries(path: String): Seq[TreeEntry]
  def entry(path: String): Seq[TreeEntry]
  def path(id: Long): Option[String]

  def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def change(changed: TreeEntry): Boolean
  def delete(key: Long): Boolean
  def delete(entry: TreeEntry): Boolean

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]
  def createDataEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit
  def nextDataid: Long

  def dataEntryExists(print: Long): Boolean
  def dataEntryExists(size: Long, print: Long): Boolean
  def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): Seq[DataEntry]

  def storeEntries(dataid: Long): Ranges
  def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit

  def settings: String Map String
  def replaceSettings(newSettings: String Map String): Unit

  def inTransaction[T](f: => T): T
  def close(): Unit
}
