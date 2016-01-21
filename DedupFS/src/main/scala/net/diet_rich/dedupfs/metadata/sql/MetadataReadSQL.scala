package net.diet_rich.dedupfs.metadata.sql

import java.sql.Timestamp

import net.diet_rich.dedupfs.metadata.{TreeEntry, MetadataReadBasic}

case class TreeQueryResult(treeEntry: TreeEntry, deleted: Boolean, id: Long, timeOfEntry: Timestamp)

// FIXME test
/** Metadata read extension methods specific for this file system implementation. */
trait MetadataReadSQL { _: MetadataReadBasic =>
  /** @return All entries, including deleted and historical entries. */
  def treeEntriesFor(key: Long): Iterator[TreeQueryResult]
  /** @return All entries that have been at one time child of the node, including deleted and historical entries. */
  def treeChildrenOf(parentKey: Long): Iterator[TreeQueryResult]
}
