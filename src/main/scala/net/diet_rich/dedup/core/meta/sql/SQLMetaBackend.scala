package net.diet_rich.dedup.core.meta.sql

import java.io.IOException

import scala.slick.jdbc.StaticQuery

import net.diet_rich.dedup.core.meta._

class SQLMetaBackend(sessionFactory: SQLSession) extends MetaBackend {
  import SQLMetaBackend._
  private implicit def session: CurrentSession = sessionFactory.session

  require(entry(rootEntry.id) == Some(rootEntry))

  override def entry(id: Long): Option[TreeEntry] = treeEntryForIdQuery(id).firstOption
  override def children(parent: Long): List[TreeEntry] = treeChildrenNotDeletedForParentQuery(parent).list
  override def children(parent: Long, name: String): List[TreeEntry] = treeChildrenNotDeletedForParentAndNameQuery(parent, name).list
  override def childrenWithDeleted(parent: Long): List[TreeEntry] = treeChildrenForParentQuery(parent).list
  override def entries(path: String): List[TreeEntry] =
    pathElements(path).foldLeft(List(rootEntry)){(nodes, name) => nodes.flatMap(node => children(node.id) filter (_.name == name))}
  override def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = inTransaction {
    val id = nextTreeEntryIdQuery.first
    createTreeEntryUpdate(id, parent, name, changed, dataid).execute
    TreeEntry(id, parent, name, changed, dataid)
  }
  override def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = inTransaction {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = inTransaction {
    val elements = pathElements(path)
    if (elements.size == 0) throw new IOException("can't create the root entry")
    val parent = elements.dropRight(1).foldLeft(rootEntry.id) { (node, childName) =>
      children(node) filter (_.name == childName) match {
        case Nil => createUnchecked(node, childName, changed, None).id
        case List(entry) => entry.id
        case entries => throw new IOException(s"ambiguous path; §entries")
      }
    }
    create(parent, elements.last, changed, dataid)
  }
  override def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = inTransaction {
    children(parent) find (_.name == name) match {
      case Some(entry) => change(entry id, parent, name, changed, dataid); entry
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }

  override def change(id: Long, newParent: Long, newName: String, newTime: Option[Long], newData: Option[Long], newDeletionTime: Option[Long]): Option[TreeEntry] = inTransaction {
    if (updateTreeEntryUpdate(newParent, newName, newTime, newData, newDeletionTime, id).first == 1)
      Some(TreeEntry(id, newParent, newName, newTime, newData, newDeletionTime))
    else None
  }
  override def markDeleted(id: Long, deletionTime: Option[Long]): Boolean = inTransaction {
    setTreeEntryDeletedUpdate(deletionTime, id).first == 1
  }

  override def dataEntry(dataid: Long): Option[DataEntry] = dataEntryForIdQuery(dataid).firstOption
  override def sizeOf(dataid: Long): Option[Long] = dataEntry(dataid) map (_ size)

  override def inTransaction[T](f: => T): T = SQLMetaBackend inTransaction f
  override def close(): Unit = sessionFactory.close()
}

object SQLMetaBackend {
  import SQLConverters._

  // TreeEntries - FIXME check whether all are used
  val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
  val sortedTreeEntriesQuery = StaticQuery.queryNA[TreeEntry](s"$selectFromTreeEntries ORDER BY id;")
  val treeEntryForIdQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
  val treeChildrenForParentQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
  val treeChildrenNotDeletedForParentQuery = StaticQuery.query[Long, TreeEntry](s"$selectFromTreeEntries WHERE parent = ? AND deleted IS NULL;")
  val treeChildrenNotDeletedForParentAndNameQuery = StaticQuery.query[(Long, String), TreeEntry](s"$selectFromTreeEntries WHERE parent = ? AND name = ? AND deleted IS NULL;")
  val nextTreeEntryIdQuery = StaticQuery.queryNA[Long]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
  val setTreeEntryDeletedUpdate = StaticQuery.update[(Option[Long], Long)]("UPDATE TreeEntries SET deleted = ? WHERE id = ?;")
  val updateTreeEntryUpdate = StaticQuery.update[(Long, String, Option[Long], Option[Long], Option[Long], Long)]("UPDATE TreeEntries SET parent = ?, name = ?, changed = ?, dataid = ?, deleted = ? WHERE id = ? AND deleted IS NULL;")
  val createTreeEntryUpdate = StaticQuery.update[(Long, Long, String, Option[Long], Option[Long])]("INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES (?, ?, ?, ?, ?);")

  // DataEntries - FIXME check whether all are used
  val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
  val dataEntryForIdQuery = StaticQuery.query[Long, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
  val dataEntriesForSizePrintQuery = StaticQuery.query[(Long, Long), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
  val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Long, Long, Array[Byte]), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
  val nextDataEntryIdQuery = StaticQuery.queryNA[Long]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")
  val createDataEntryUpdate = StaticQuery.update[(Long, Long, Long, Array[Byte], Int)]("INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")

  // Note: Writing is synchronized, so "create only if not exists" can be implemented.
  def inTransaction[T](f: => T): T = synchronized(f)
}