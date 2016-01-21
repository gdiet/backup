package net.diet_rich.dedupfs.metadata.sql

import java.sql.Timestamp

import net.diet_rich.dedupfs.metadata.{TreeEntry, MetadataReadBasic}

case class TreeQueryResult(treeEntry: TreeEntry, deleted: Boolean, id: Long, timeOfEntry: Timestamp)

/** Metadata read extension methods specific for this file system implementation. */
trait MetadataReadSQL { _: MetadataReadBasic =>
  // FIXME return the result case class
  /** @return Deleted and/or historical entries */
  def treeEntriesFor(key: Long): Iterator[TreeQueryResult]
  /** @return Deleted and/or historical entries */
  def treeChildrenOf(parentKey: Long, filterDeleted: Option[Boolean] = Some(false), upToId: Long = Long.MaxValue): Iterable[TreeEntry]

  // FIXME methods to access entry ID and timestamp
  // FIXME test
}
