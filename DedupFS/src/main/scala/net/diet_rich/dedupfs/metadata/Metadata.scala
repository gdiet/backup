package net.diet_rich.dedupfs.metadata

import TreeEntry.RichPath

trait MetadataRead extends AutoCloseable {
  def entry(key: Long): Option[TreeEntry]
  /** @return all children, may include multiple children with the same name. */
  def allChildren(parent: Long): Iterable[TreeEntry]
  /** @return children with unique names. */
  def children(parent: Long): Iterable[TreeEntry]
  /** @return all children with the name provided. */
  def allChildren(parent: Long, name: String): Iterable[TreeEntry]
  /** @return one child with the name provided (if any). */
  def child(parent: Long, name: String): Option[TreeEntry]

  /** @return one entry with the path provided (if any). */
  final def entry(path: String): Option[TreeEntry] = entry(path.pathElements)
  /** @return one entry with the path provided (if any). */
  def entry(path: Array[String]): Option[TreeEntry]
  /** @return all entries with the path provided, possibly including multiple path elements with the same names. */
  final def allEntries(path: String): Iterable[TreeEntry] = allEntries(path.pathElements)
  /** @return all entries with the path provided, possibly including multiple path elements with the same names. */
  def allEntries(path: Array[String]): Iterable[TreeEntry]

  def path(key: Long): Option[String]

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]

  def dataEntryExists(print: Long): Boolean
  def dataEntryExists(size: Long, print: Long): Boolean
  def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): Seq[DataEntry]

  def storeEntries(dataid: Long): Ranges

  def settings: String Map String
  def hashAlgorithm: String

  def close(): Unit
}

trait Metadata extends MetadataRead {
  def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def change(changed: TreeEntry): Boolean
  def delete(key: Long): Boolean
  def delete(entry: TreeEntry): Unit

  def createDataEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit
  def nextDataid(): Long

  def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit

  def replaceSettings(newSettings: String Map String): Unit

  def inTransaction[T](f: => T): T
}
