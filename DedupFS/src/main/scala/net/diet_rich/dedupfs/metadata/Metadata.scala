package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.Print

import TreeEntry.RichPath

trait MetadataRead extends MetadataReadBasic with MetadataReadExtended

trait MetadataReadBasic extends AutoCloseable {
  def entry(key: Long): Option[TreeEntry]
  def children(parent: Long): Iterable[TreeEntry]
  def child(parent: Long, name: String): Option[TreeEntry]

  final def entry(path: String): Option[TreeEntry] = entry(path.pathElements)
  def entry(path: Array[String]): Option[TreeEntry]

  def path(key: Long): Option[String]

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]

  def dataEntryExists(print: Print): Boolean
  def dataEntryExists(size: Long, print: Print): Boolean
  def dataEntriesFor(size: Long, print: Print, hash: Array[Byte]): Seq[DataEntry]

  def storeEntries(dataid: Long): Ranges

  def settings: String Map String
  def hashAlgorithm: String
}

trait MetadataReadExtended {
  /** @return all children, may include multiple children with the same name. */
  def allChildren(parent: Long): Iterable[TreeEntry]
  /** @return all children with the name provided. */
  def allChildren(parent: Long, name: String): Iterable[TreeEntry]
  /** @return all entries with the path provided, possibly including multiple path elements with the same names. */
  final def allEntries(path: String): Iterable[TreeEntry] = allEntries(path.pathElements)
  /** @return all entries with the path provided, possibly including multiple path elements with the same names. */
  def allEntries(path: Array[String]): Iterable[TreeEntry]
  /** @return deleted and/or historical entries */
  def treeEntryFor(key: Long, isDeleted: Boolean = false, upToId: Long = Long.MaxValue): Option[TreeEntry]
  /** @return deleted and/or historical entries */
  def treeChildrenOf(parentKey: Long, isDeleted: Boolean = false, upToId: Long = Long.MaxValue): Iterable[TreeEntry]
}

trait Metadata extends MetadataRead {
  def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def changeUnchecked(changed: TreeEntry): Unit
  def change(changed: TreeEntry): Boolean
  final def change(key: Long, parent: Option[Long], name: Option[String], changed: Option[Option[Long]], data: Option[Option[Long]]): Boolean = inTransaction {
    entry(key) match {
      case None => false
      case Some(entry) =>
        changeUnchecked(TreeEntry(
          key     = key,
          parent  = parent getOrElse entry.parent,
          name    = name getOrElse entry.name,
          changed = changed getOrElse entry.changed,
          data    = data getOrElse entry.data
        ))
        true
    }
  }
  def delete(key: Long): Boolean
  def delete(entry: TreeEntry): Unit

  def createDataEntry(reservedid: Long, size: Long, print: Print, hash: Array[Byte], storeMethod: Int): Unit
  def nextDataid(): Long

  def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit

  def replaceSettings(newSettings: String Map String): Unit

  def inTransaction[T](f: => T): T
}
