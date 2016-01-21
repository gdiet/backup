package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.vals.Print
import net.diet_rich.dedupfs.hashAlgorithmKey

import TreeEntry.RichPath

trait MetadataRead extends MetadataReadBasic with MetadataReadUtils

/** Basic file system metadata read methods. Refers only to the current state of entries not marked deleted. */
trait MetadataReadBasic extends AutoCloseable {

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

/** Utility extensions of the basic metadata read methods. */
trait MetadataReadUtils { _: MetadataReadBasic =>
  /** Returns child nodes by name. Returns multiple children with the same name if present. May return children
    * of entries marked deleted, if the children are not marked deleted.
    *
    * @return The tree entry's children named as requested. */
  def child(parent: Long, name: String): Iterable[TreeEntry] =
    children(parent) filter (_.name == name)

  /** @return The tree entries reachable by the path. */
  def entry(path: String): Iterable[TreeEntry] =
    entry(path.pathElements)
  /** @return The tree entries reachable by the path. */
  def entry(path: Array[String]): Iterable[TreeEntry] =
    path.foldLeft(Iterable(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }

  /** @return The path for a tree entry or `None` if it does not exist. */
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

// FIXME MetadataWrite should not require MetadataReadExtended
trait Metadata extends MetadataRead {
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
