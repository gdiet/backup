package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.vals.Print
import net.diet_rich.dedupfs.hashAlgorithmKey

import TreeEntry.RichPath

trait MetadataReadAll extends MetadataRead with MetadataReadUtils with MetadataReadSpecial

/** Basic file system metadata read methods. Refers only to the current state of entries not marked deleted. */
trait MetadataRead extends AutoCloseable {

  /** Returns the tree entry or `None` if the entry does not exist or is marked deleted. */
  def entry(key: Long): Option[TreeEntry]

  /** Returns the tree entry's children. Returns multiple children with the same name if present. May return children
    * of entries marked deleted, if the children are not marked deleted.
    *
    * @return The tree entry's children. */
  def children(parent: Long): Iterable[TreeEntry]

  /** Returns the data entry or `None` if it does not exist. */
  def dataEntry(dataid: Long): Option[DataEntry]

  /** Returns `true` if a corresponding data entry exists. */
  def dataEntryExists(print: Print): Boolean
  /** Returns `true` if a corresponding data entry exists. */
  def dataEntryExists(size: Long, print: Print): Boolean
  /** Returns all (that is, normally 0 or 1) corresponding data entries. */
  def dataEntriesFor(size: Long, print: Print, hash: Array[Byte]): Seq[DataEntry]

  /** Returns the actual data store pointers for a data entry. */
  def storeEntries(dataid: Long): Ranges

  /** Returns the settings map for this metadata repository. */
  def settings: String Map String
}

// FIXME continue utility methods collection
/** Utility extensions of the basic metadata read methods. */
trait MetadataReadUtils { _: MetadataRead =>
  /** Returns child nodes by name. Returns multiple children with the same name if present. May return children
    * of entries marked deleted, if the children are not marked deleted.
    *
    * @return The tree entry's children named as requested. */
  def child(parent: Long, name: String): Iterable[TreeEntry] =
    children(parent) filter (_.name == name)

  def entry(path: String): Iterable[TreeEntry] =
    entry(path.pathElements)
  def entry(path: Array[String]): Iterable[TreeEntry] =
    path.foldLeft(Iterable(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }

  def path(key: Long): Option[String] =
    if (key == TreeEntry.root.key) Some(TreeEntry.rootPath)
    else entry(key) flatMap {entry => path(entry.parent) map (_ + TreeEntry.pathSeparator + entry.name)}

  /** Returns the data size of a data entry if the entry it exists. */
  def sizeOf(dataid: Long): Option[Long] =
    dataEntry(dataid) map (_.size)

  /** Returns the hash algorithm setting for this metadata repository. */
  def hashAlgorithm: String =
    settings(hashAlgorithmKey)
}

/** Metadata read extension methods specific for this file system implementation. */
trait MetadataReadSpecial {
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
