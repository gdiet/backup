package net.diet_rich.dedup.meta.h2

import java.sql.ResultSet

import net.diet_rich.common.RichTraversableLike
import net.diet_rich.common.sql.{ConnectionFactory, query, WrappedSQLResult}
import net.diet_rich.dedup.meta.{MetaBackend, TreeEntry}

class H2MetaBackend(directory: java.io.File, repositoryId: String) extends MetaBackend { import H2MetaBackend._
  private implicit val connectionFactory: ConnectionFactory = ???

  private val prepTreeQueryByKey = {
    implicit val treeEntryResult: ResultSet => TreeEntry = { r => TreeEntry(r long 1, r long 2 , r string 3, r longOption 4, r longOption 5) }
    implicit val treeQueryResult: ResultSet => TreeQueryResult = { r => TreeQueryResult(treeEntryResult(r), r boolean 6, r long 7, r long 8) }
    query[TreeQueryResult]("SELECT key, parent, name, changed, dataId, deleted, id, timestamp FROM TreeEntries WHERE key = ?")
  }

  override def entry(key: Long): Option[TreeEntry] =
    prepTreeQueryByKey.runv(key).toSeq
      .maxOptionBy(_.id)            // Only the latest version of the entry
      .filter(_.deleted == false)   // Filter by deleted status
      .map(_.treeEntry)             // Get the entry

  override def children(parent: Long): Iterable[TreeEntry] = ???

  override def settings: String Map String = ???
}

object H2MetaBackend {
  private case class TreeQueryResult(treeEntry: TreeEntry, deleted: Boolean, id: Long, timeOfEntry: Long)
}

