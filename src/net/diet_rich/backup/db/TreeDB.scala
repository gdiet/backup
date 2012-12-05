// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.db

import net.diet_rich.util.sql._
import java.sql.Connection
import net.diet_rich.util.Executor

case class TreeEntry(id: Long, parent: Long, name: String, time: Long, dataid: Long)

/** Basic tree db interface used for tree data access. */
trait BaseTreeDB { import TreeDB._
  /** @return The tree entry, None if no such node. */
  def entry(id: Long): Option[TreeEntry]
  /** @return The children, empty if no such node. */
  def children(id: Long): Iterable[TreeEntry]
  /** @return The entry ID. */
  def create(parent: Long, name: String, time: Long, data: Long): Long
  /** Does nothing if no such node. */
  def rename(id: Long, newName: String): Unit
  /** Does nothing if no such node. */
  def setTime(id: Long, newTime: Long): Unit
  /** Does nothing if no such node. */
  def setData(id: Long, newData: Long): Unit
  /** Does nothing if no such node. */
  def move(id: Long, newParent: Long): Unit
  /** Does nothing if no such node. */
  def deleteWithChildren(id: Long): Unit
}

/** Convenience features for the tree db interface. */
trait TreeDB extends BaseTreeDB { import TreeDB._
  /** @return The entry ID. */
  def create(parent: Long, name: String): Long = create(parent, name, NOTIME, NODATAID)
  /** @return The child's entry ID if any. */
  def childId(parentId: Long, name: String): Option[Long] = children(parentId).find(_.name == name).map(_.id)
  /** @return The entry ID. Missing path elements are created on the fly. */
  def getOrMake(path: String): Long = {
    require(path.startsWith("/"), "Path <%s> does not start with '/'" format path)
    val parts = path.split("/").drop(1)
    parts.foldLeft(ROOTID) {(node, childName) =>
      val childOption = children(node).filter(_.name == childName).headOption;
      childOption map(_.id) getOrElse create(node, childName)
    }
  }
  /** @return The entry or None if no such entry. */
  def entry(path: String): Option[TreeEntry] = {
    require(path.startsWith("/"), "Path <%s> does not start with '/'" format path)
    val parts = path.split("/").drop(1)
    parts.foldLeft(entry(ROOTID)) {(node, childName) =>
      node flatMap(node => children(node.id).filter(_.name == childName).headOption);
    }
  }
  /** @return true if the child exists. */
  def childExists(id: Long, childName: String) = ! children(id).filter(_.name == childName).isEmpty
  
  // the methods below could be moved to a separate object since they do not access any tree data
  /** @return the parent path for a path. */
  def parentPath(path: String): String = {
    val interim = path substring (0, path lastIndexOf "/")
    if (interim == ROOTNAME) ROOTPATH else interim
  }
  /** @return the element name for a path, ROOTNAME for the root. */
  def nameFromPath(path: String) = if (path == ROOTPATH) ROOTNAME else path substring (1 + path lastIndexOf "/", path.length)
}

object TreeDB {
  val ROOTPARENT = -1L
  val ROOTID = 0L
  val ROOTNAME = ""
  val ROOTPATH = "/"
  val DELETEDROOT = -1L
  val NOTIME = -1L
  val NODATAID = -1L
  
  def standardDB(implicit connection: Connection): TreeDB =
    new TreeSqlDB with TreeDB
    
  def deferredInsertDB(sqlExecutor: Executor)(implicit connection: Connection): TreeDB =
    new TreeSqlDB with TreeDB {
      protected override def doAddEntry(id: Long, parent: Long, name: String, time: Long, data: Long) = 
        sqlExecutor(super.doAddEntry(id, parent, name, time, data))
    }
}

object TreeSqlDB {
  import TreeDB._
  
  def createTable(connection: Connection) : Unit = {
    // The tree is represented by nodes that store their parent but not their children.
    // There are not technical restrictions against illegal node types, e.g.
    // multiple children with the same name, or entries with illegal parent entry.
    // The tree root must not be deleted, has the ID 0 and parent 0.
    // dataid should be 0 for 0-byte TreeEntries
    // dataid should be -1, but must be negative for directory TreeEntries
    execUpdate(connection, """
      CREATE CACHED TABLE TreeEntries (
        id     BIGINT PRIMARY KEY,
        parent BIGINT NOT NULL,
        name   VARCHAR(256) NOT NULL,
        time   BIGINT DEFAULT %s NOT NULL,
        dataid BIGINT DEFAULT %s NOT NULL
      );
    """ format (NOTIME, NODATAID))
    execUpdate(connection, "CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);")
    execUpdate(connection, "CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);")
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?);", ROOTID, ROOTPARENT, ROOTNAME)
  }

  def dropTable(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE TreeEntries IF EXISTS;")

  // used as index usage markers
  def idxParent[T](t : T) = t
  def idxDataid[T](t : T) = t
}

class TreeSqlDB(implicit connection: Connection) extends BaseTreeDB { import TreeDB._

  protected val queryEntry =
    prepareQuery("SELECT parent, name, time, dataid FROM TreeEntries WHERE id = ?;")
  override def entry(id: Long): Option[TreeEntry] =
    queryEntry(id) { r =>
      TreeEntry(id, r long 1, r string 2, r long 3, r long 4)
    }.nextOptionOnly

  protected val queryChildren = TreeSqlDB idxParent
    prepareQuery("SELECT id, name, time, dataid FROM TreeEntries WHERE parent = ?;")
  override def children(parent: Long): Iterable[TreeEntry] =
    queryChildren(parent) { r =>
      TreeEntry(r long 1, parent, r string 2, r long 3, r long 4)
    }.toList
    
  
  protected val renameEntry =
    prepareUpdate("UPDATE TreeEntries SET name = ? WHERE id = ?;")
  override def rename(id: Long, newName: String): Unit =
    renameEntry(newName, id)
    
  protected val maxEntryId = 
    SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM TreeEntries;")
    
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES (?, ?, ?, ?, ?);")
  /** can be overridden to run in a different thread. */
  protected def doAddEntry(id: Long, parent: Long, name: String, time: Long, data: Long): Unit = 
    addEntry(id, parent, name, time, data)
  override final def create(parent: Long, name: String, time: Long, data: Long): Long = {
    val id = maxEntryId incrementAndGet()
    doAddEntry(id, parent, name, time, data)
    id
  }

  protected val changeTime = 
    prepareUpdate("UPDATE TreeEntries SET time = ? WHERE id = ?;")
  override def setTime(id: Long, newTime: Long): Unit =
    changeTime(newTime, id)

  protected val changeData = 
    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?;")
  override def setData(id: Long, newData: Long): Unit =
    changeData(newData, id)

  protected val moveEntry =
    prepareUpdate("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
  override def move(id: Long, newParent: Long): Unit =
    moveEntry(newParent, id)

  protected val deleteEntry =
    prepareUpdate("DELETE FROM TreeEntries WHERE id = ?;")
  /** can be overridden to run in a different thread. */
  protected def doDeleteChildren(id: Long): Unit = {
    deleteEntry(id)
    children(id) foreach { entry => doDeleteChildren(entry.id) }
  }
  override def deleteWithChildren(id: Long) : Unit = {
    deleteEntry(id)
    doDeleteChildren(id)
  }
}
