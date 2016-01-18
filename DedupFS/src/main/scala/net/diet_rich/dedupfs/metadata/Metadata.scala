package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.vals.Print

import TreeEntry.RichPath

trait MetadataReadAll extends MetadataRead with MetadataReadUtils with MetadataReadSpecial

/** Basic file system metadata read methods. */
trait MetadataRead extends AutoCloseable {
  /** @return The tree entry's current state or none if it does not exist or is deleted. */
  def entry(key: Long): Option[TreeEntry]
  /** @return The current states of the tree entry's non-deleted children.
    *         This method is indifferent to the deleted state of the tree entry.
    *         For multiple children with the same name returns one each. */
  def children(parent: Long): Iterable[TreeEntry]

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

/** Utility extensions of the basic metadata read methods. */
trait MetadataReadUtils { _: MetadataRead =>
  // FIXME continue utility methods collection
  def child(parent: Long, name: String): Option[TreeEntry] = children(parent) find (_.name == name)
}

/** Metadata read extension methods specific for this file system implementation. */
trait MetadataReadSpecial {
  /** @return All children, may include multiple children with the same name. */
  def allChildren(parent: Long): Iterable[TreeEntry]
  /** @return All children with the name provided. */
  def allChildren(parent: Long, name: String): Iterable[TreeEntry]
  /** @return All entries with the path provided, possibly including multiple path elements with the same names. */
  final def allEntries(path: String): Iterable[TreeEntry] = allEntries(path.pathElements)
  /** @return All entries with the path provided, possibly including multiple path elements with the same names. */
  def allEntries(path: Array[String]): Iterable[TreeEntry]
  /** @return Deleted and/or historical entries */
  def treeEntryFor(key: Long, filterDeleted: Option[Boolean] = Some(false), upToId: Long = Long.MaxValue): Option[TreeEntry]
  /** @return Deleted and/or historical entries */
  def treeChildrenOf(parentKey: Long, filterDeleted: Option[Boolean] = Some(false), upToId: Long = Long.MaxValue): Iterable[TreeEntry]

  // FIXME methods to access entry ID and timestamp
}

// FIXME MetadataWrite should not require MetadataReadExtended
trait Metadata extends MetadataReadAll {
  def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long
  def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long
  def changeUnchecked(changed: TreeEntry): Unit
  def change(changed: TreeEntry): Boolean
  final def change(key: Long, parent: Option[Long], name: Option[String], changed: Option[Option[Long]], data: Option[Option[Long]]): Boolean = serialized {
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

  def serialized[T](f: => T): T
}
