// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.fdfs

import net.diet_rich.util.sql._
import java.sql.Connection

case class TreeEntry(id: Long, parent: Long, name: String, time: Long, dataid: Long)

trait TreeDB {
  import TreeDB._
  /** @return The tree entry, None if no such node. */
  def entry(id: Long) : Option[TreeEntry]
  /** @return The children, empty if no such node. */
  def children(id: Long) : Iterable[TreeEntry]
  /** @return The entry ID. */
  def create(parent: Long, name: String, time: Long = NOTIME, data: Long = NODATAID) : Long
  /** Does nothing if no such node. */
  def rename(id: Long, newName: String) : Unit
  /** Does nothing if no such node. */
  def setTime(id: Long, newTime: Long) : Unit
  /** Does nothing if no such node. */
  def setData(id: Long, newData: Long) : Unit
  /** Does nothing if no such node. */
  def move(id: Long, newParent: Long) : Unit
  /** Does nothing if no such node. */
  def deleteWithChildren(id: Long) : Unit
}

object TreeDB {
  val ROOTID = 0L
  val ROOTNAME = ""
  val ROOTPATH = ""
  val DELETEDROOT = -1L
  val NOTIME = -1L
  val NODATAID = -1L
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
    // from http://hsqldb.org/doc/guide/databaseobjects-chapt.html
    // PRIMARY KEY, UNIQUE or FOREIGN key constraints [... create] an index automatically.
    execUpdate(connection, "CREATE INDEX idxParent ON TreeEntries(parent);")
    execUpdate(connection, "CREATE INDEX idxDataid ON TreeEntries(dataid);")
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?);", ROOTID, ROOTID, ROOTNAME)
  }
  
  // used as index usage markers
  def idxParent[T](t : T) = t
  def idxDataid[T](t : T) = t

  def setupDB(connection: Connection) : TreeDB = new TreeSqlDB()(connection)
  def setupDeferredInsertDB(connection: Connection, executor: SqlDBCommon.Executor) : TreeDB =
    new DeferredInsertTreeDB(connection, executor)

}

protected[fdfs] class TreeSqlDB(protected implicit val connection: Connection) extends SqlDBCommon with TreeDB {
  import TreeSqlDB._
  import TreeDB._

  protected val queryEntry =
    prepareQuery("SELECT parent, name, time, dataid FROM TreeEntries WHERE id = ?;")
  override def entry(id: Long) : Option[TreeEntry] =
    queryEntry(id) { result =>
      TreeEntry(id, result long 1, result string 2, result long 3, result long 4)
    } headOption

  protected val queryChildren = 
    idxParent(prepareQuery("SELECT id, name, time, dataid FROM TreeEntries WHERE parent = ?;"))
  override def children(parent: Long) : Iterable[TreeEntry] =
    queryChildren(parent) { result =>
      TreeEntry(result long 1, parent, result string 2, result long 3, result long 4)
    } toList
    
  
  protected val renameEntry =
    prepareUpdate("UPDATE TreeEntries SET name = ? WHERE id = ?;")
  override def rename(id: Long, newName: String) : Unit =
    renameEntry(newName, id)
    
  protected val maxEntryId = 
    readAsAtomicLong("SELECT MAX(id) FROM TreeEntries;")
    
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES (?, ?, ?, ?, ?);")
  protected def doAddEntry(id: Long, parent: Long, name: String, time: Long, data: Long): Unit = 
    addEntry(id, parent, name, time, data)
  override final def create(parent: Long, name: String, time: Long, data: Long): Long = {
    val id = maxEntryId incrementAndGet()
    doAddEntry(id, parent, name, time, data)
    id
  }

  protected val changeTime = 
    prepareUpdate("UPDATE TreeEntries SET time = ? WHERE id = ?;")
  override def setTime(id: Long, newTime: Long) : Unit =
    changeTime(newTime, id)

  protected val changeData = 
    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?;")
  override def setData(id: Long, newData: Long) : Unit =
    changeData(newData, id)

  protected val moveEntry =
    prepareUpdate("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
  override def move(id: Long, newParent: Long) : Unit =
    moveEntry(newParent, id)

  protected val deleteEntry =
    prepareUpdate("DELETE FROM TreeEntries WHERE id = ?;")
  override def deleteWithChildren(id: Long) : Unit = {
    deleteEntry(id)
    // eventually, move to a separate thread
    def innerRecurse(id: Long) : Unit = {
      children(id) foreach { entry => innerRecurse(entry id) }
      deleteEntry(id)
    }
    innerRecurse(id)
  }
}

protected[fdfs] class DeferredInsertTreeDB(
      connection: Connection,
      executor: SqlDBCommon.Executor
    ) extends TreeSqlDB()(connection) {
  import net.diet_rich.util.closureToRunnable

  // eventually, we could think about SQL batch execution
  // and turning autocommit off to get a few percent higher performance

  protected override def doAddEntry(id: Long, parent: Long, name: String, time: Long, data: Long) = 
    executor.execute { addEntry(id, parent, name, time, data) }
}


