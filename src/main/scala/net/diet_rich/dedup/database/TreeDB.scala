// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import net.diet_rich.util.{Hash, AugmentedString}
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

case class TreeEntry(
  id: TreeEntryID,
  parent: Option[TreeEntryID],
  name: String,
  nodeType: NodeType,
  time: Time = Time(0),
  deleted: Option[Time] = None,
  dataid: Option[DataEntryID] = None)

trait RespectDeleted {
  implicit protected val connection: Connection
  protected final val queryEntry = 
    prepareQuery(
      "SELECT parent, name, type, time, deleted, dataid FROM TreeEntries WHERE id = ? AND deleted IS NULL",
      "the not-deleted tree entry for id %d"
    )
  protected final val queryChild: SqlQuery = 
    prepareQuery(
      "SELECT id, type, time, deleted, dataid FROM TreeEntries WHERE parent = ? AND name = ? AND deleted IS NULL",
      "the not-deleted tree entry for parent %d with name %s"
    )
  protected final val queryChildren =
    prepareQuery(
      "SELECT id, name, type, time, deleted, dataid FROM TreeEntries WHERE parent = ? AND deleted IS NULL",
      "the not-deleted children of tree entry %d"
    )
}

trait IgnoreDeleted {
  implicit protected val connection: Connection
  protected final val queryEntry = 
    prepareQuery(
      "SELECT parent, name, type, time, deleted, dataid FROM TreeEntries WHERE id = ?",
      "any tree entry for id %d"
    )
  protected final val queryChild: SqlQuery = 
    prepareQuery(
      "SELECT id, type, time, deleted, dataid FROM TreeEntries WHERE parent = ? AND name = ?",
      "any tree entries for parent %d with name %s"
    )
  protected final val queryChildren =
    prepareQuery(
      "SELECT id, name, type, time, deleted, dataid FROM TreeEntries WHERE parent = ?",
      "all children of tree entry %d"
    )
}

trait TreeDB { import TreeDB._
  implicit protected val connection: Connection

  val ROOTID: TreeEntryID =
    TreeEntryID(query("SELECT id FROM TreeEntries WHERE id = parent")(_ long 1) nextOnly)
  
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: TreeEntryID, name: String, nodeType: NodeType, time: Time = Time(0), dataId: Option[DataEntryID] = None): TreeEntryID =
    TreeEntryID(addEntry(parentId.value, name, nodeType.value, time.value, dataId.map(_.value)))
  protected final val addEntry = 
    prepareInsertReturnKey("INSERT INTO TreeEntries (parent, name, type, time, dataid) VALUES (?, ?, ?, ?, ?)")

  /** @return The entry if any. */
  def entry(id: TreeEntryID): Option[TreeEntry] =
    queryEntry(id.value)(
      r => TreeEntry(id, TreeEntryID(r longOption 1), r string 2, NodeType(r int 3), Time(r long 4), Time(r longOption 5), DataEntryID(r longOption 6))
    ).nextOptionOnly
  protected val queryEntry: SqlQuery
  
  /** @return The child entry if any. */
  def child(parent: TreeEntryID, name: String): Option[TreeEntry] =
    queryChild(parent.value, name)(
      // TODO use one partial function for getting a tree entry from a result set
      r => TreeEntry(TreeEntryID(r long 1), Some(parent), name, NodeType(r int 2), Time(r long 3), Time(r longOption 4), DataEntryID(r longOption 5))
    ).toSeq filterNot(_.id == ROOTID) headOption
  protected val queryChild: SqlQuery
  
  /** Note: If the children are not consumed immediately, they must be stored e.g. by calling *.toList.
   *  @return The children, empty if no such node. */
  def children(parent: TreeEntryID): Seq[TreeEntry] =
    queryChildren(parent.value)(
      r => TreeEntry(TreeEntryID(r long 1), Some(parent), r string 2, NodeType(r int 3), Time(r long 4), Time(r longOption 5), DataEntryID(r longOption 6))
    ).toSeq filterNot(_.id == ROOTID)
  protected val queryChildren : SqlQuery
    
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: TreeEntryID): Option[FullDataInformation] =
    queryFullDataInformation(id.value)(
      r => FullDataInformation(Time(r long 1), Size(r long 2), Print(r long 3), Hash(r bytes 4), DataEntryID(r longOption 5))
    ).nextOptionOnly
  protected val queryFullDataInformation = prepareQuery(
    "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo " +
    "ON TreeEntries.dataid = DataInfo.id AND TreeEntries.id = ?",
    "the full data information from tree and data info for tree id %d"
  )
  
  /** @return true if the deleted flag was set for the entry. */
  def markDeleted(id: TreeEntryID): Boolean =
    (id != ROOTID) && (markEntryDeleted(System currentTimeMillis, id value) match {
      case 0 => false
      case 1 => true
      case n => throw new AssertionError(s"markDeleted: $n entries updated for $id")
    })
  protected val markEntryDeleted = prepareUpdate(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  
  /** @return true if the entry was updated. */
  def changePath(id: TreeEntryID, newName: String, newParent: TreeEntryID): Boolean = {
    (id != ROOTID) && (updatePath(newName, newParent.value, id.value) match {
      case 0 => false
      case 1 => true
      case n => throw new AssertionError(s"changePath: $n entries updated for $id")
    })
  }
  protected final val updatePath = prepareUpdate(
    "UPDATE TreeEntries SET name = ?, parent = ? WHERE id = ?"
  )
}

trait TreeDBUtils { self: TreeDB => import TreeDB._
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID] =
    child(parent, name).map(_.id)
  
  def path(id: TreeEntryID): Option[Path] = {
    if (id == ROOTID) Some(ROOTPATH) else {
      entry(id) flatMap { entry =>
        path(entry.parent.get).map(_ + SEPARATOR + entry.name)
      }
    }
  }

  /** @return The entry ID. Missing path elements are created on the fly. */
  def getOrMakeDir(path: Path): TreeEntryID = if (path == ROOTPATH) ROOTID else {
    assume(path.value.startsWith(SEPARATOR), s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value.split(SEPARATOR).drop(1)
    parts.foldLeft(ROOTID) {(node, childName) =>
      val childOption = children(node).find(_.name == childName)
      childOption map(_.id) getOrElse createAndGetId(node, childName, NodeType.DIR)
    }
  }

  /** @return The entry or None if no such entry. */
  def entry(path: Path): Option[TreeEntry] = if (path == ROOTPATH) entry(ROOTID) else {
    assume(path.value.startsWith(SEPARATOR), s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value.split(SEPARATOR).drop(1)
    parts.foldLeft(entry(ROOTID)) {(node, childName) =>
      node.flatMap(node => children(node.id).find(_.name == childName))
    }
  }
  
  /** @return The entry or None if no such entry. */
  def entryWithWildcards(path: Path): Option[TreeEntry] = if (path == ROOTPATH) entry(ROOTID) else {
    assume(path.value.startsWith(SEPARATOR), s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value.split(SEPARATOR).drop(1)
    parts.foldLeft(entry(ROOTID)) {(node, pathElement) =>
      val childNameRegexp = pathElement processSpecialSyntax (java.util.regex.Pattern.quote(_), identity)
      node.flatMap(node => children(node.id).toList.sortBy(_.name).reverse.find(_.name.matches(childNameRegexp)))
    }
  }
}

object TreeDB {
  val ROOTPATH = Path("") // FIXME make or add ROOTNAME
  val SEPARATOR = "/"

  def createTable(implicit connection: Connection): Unit = {
    update("CREATE SEQUENCE treeEntriesIdSeq;")
    update("""
      CREATE TABLE TreeEntries (
        id      BIGINT DEFAULT (NEXT VALUE FOR treeEntriesIdSeq) PRIMARY KEY,
        parent  BIGINT NOT NULL,
        name    VARCHAR(256) NOT NULL,
        type    INTEGER NOT NULL,
        time    BIGINT NOT NULL DEFAULT 0,
        deleted BIGINT DEFAULT NULL,
        dataid  BIGINT DEFAULT NULL
      );
    """ normalizeMultiline)
    recreateIndexes
    val rootId: Long = query("SELECT NEXT VALUE FOR treeEntriesIdSeq;")(_ long 1).next
    update(s"INSERT INTO TreeEntries (id, parent, name, type) VALUES ($rootId, $rootId, '${ROOTPATH.value}', ${NodeType.DIR.value})")
    // TODO use implicit conversions for e.g. NodeType to Int (useful almost everywhere)
  }

  def recreateIndexes(implicit connection: Connection): Unit = {
    update("DROP INDEX idxTreeEntriesParent IF EXISTS;")
    update("DROP INDEX idxTreeEntriesDataid IF EXISTS;")
    update("DROP INDEX idxTreeEntriesDeleted IF EXISTS;")
    
    update("CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);")
    update("CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);")
    update("CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);")
  }
  
  def dropTable(implicit connection: Connection): Unit = {
    update("DROP TABLE TreeEntries IF EXISTS;")
    update("DROP SEQUENCE treeEntriesIdSeq IF EXISTS;")
  }

}
