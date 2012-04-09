// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import TreeDB._
import df.IdAndName
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._
import scala.collection.immutable.Iterable
import java.util.concurrent.atomic.AtomicLong
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import scala.collection.mutable.SynchronizedQueue

class TreeSqlDB(connection: Connection) extends TreeDB with TreeCacheUpdater with TreeDataUpdater {
  var treeAdapters = new SynchronizedQueue[TreeCacheUpdateAdapter]
  override def registerUpdateAdapter(adapter: TreeCacheUpdateAdapter) = treeAdapters += adapter

  var dataAdapters = new SynchronizedQueue[TreeDataUpdateAdapter]
  override def registerUpdateAdapter(adapter: TreeDataUpdateAdapter) = dataAdapters += adapter
  
  def prepare(statement: String) : ScalaThreadLocal[PreparedStatement] =
    ScalaThreadLocal(connection prepareStatement statement, statement)
  val maxEntryId: AtomicLong = new AtomicLong(
    execQuery(connection, "SELECT MAX ( id ) AS id FROM TreeEntries;")(_ long 1 ) headOnly
  )
  val childrenForId_ = prepare("SELECT id, name FROM TreeEntries WHERE parent = ?;")
  val nameForId_ = prepare("SELECT name FROM TreeEntries WHERE id = ?;")
  val parentForId_ = prepare("SELECT parent FROM TreeEntries WHERE id = ?;")
  val dataForId_ = prepare("SELECT dataid FROM TreeEntries WHERE id = ?;")
  val addEntry_ = prepare("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
  val renameEntry_ = prepare("UPDATE TreeEntries SET name = ? WHERE id = ?;")
  val moveEntry_ = prepare("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
  val deleteEntry_ = prepare("DELETE FROM TreeEntries WHERE id = ?;")

  override def name(id: Long) : Option[String] =
    execQuery(nameForId_, id)(_ string 1) headOption
  override def children(id: Long) : Iterable[IdAndName] =
    execQuery(childrenForId_, id)(result => IdAndName(result long 1, result string 2)) toList
  override def parent(id: Long) : Option[Long] =
    execQuery(parentForId_, id)(_ long 1) headOption
  override def createNewNode(parent: Long, name: String) : Option[Long] = {
    // This method MUST check that the parent exists and there is no child with the same name.
    val id = maxEntryId incrementAndGet()
    try { execUpdate(addEntry_, id, parent, name) match {
      // EVENTUALLY, the update adapters should be called from a separate thread
      case 1 => treeAdapters foreach(_ created (id, name, parent)); Some(id)
      case n => throw new IllegalStateException("Create: Unexpected %s times update for id %s" format(n, id))
      // EVENTUALLY, the exception could be inspected more in detail
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); None }
  }
  override def rename(id: Long, newName: String) : Boolean = {
    // This method MUST check that there is no sibling with the same name.
    try { execUpdate(renameEntry_, newName, id) match {
      // EVENTUALLY, the update adapters should be called from a separate thread
      case 0 => false
      case 1 => treeAdapters foreach(_ renamed (id, newName)); true
      case n => throw new IllegalStateException("Rename: Unexpected %s times update for id %s" format(n, id))
      // EVENTUALLY, the exception could be inspected more in detail
    } } catch { case e: SQLException => false }
  }
  override def move(id: Long, newParent: Long) : Boolean = {
    val oldParent = parent(id) // FIXME take from cache
    // This method MUST check that there will be no sibling with the same name.
    try { execUpdate(moveEntry_, newParent, id) match {
      // EVENTUALLY, the update adapters should be called from a separate thread
      case 0 => false
      case 1 => treeAdapters foreach(_ moved (id, oldParent get, newParent)); true
      case n => throw new IllegalStateException("Move: Unexpected %s times update for id %s" format(n, id))
      // EVENTUALLY, the exception could be inspected more in detail
    } } catch { case e: SQLException => false }
  }
  override def deleteWithChildren(id: Long) : Boolean = {
    val oldParent = parent(id) // FIXME take from cache
    val markedDeleted = try { execUpdate(moveEntry_, DELETEDROOT, id) match {
      case 0 => false
      case 1 => true
      case n => throw new IllegalStateException("Delete: Unexpected %s times update for id %s" format(n, id))
      // EVENTUALLY, the exception could be inspected more in detail
    } } catch { case e: SQLException => false }
    if (markedDeleted) {
      // EVENTUALLY, the following should be called from a separate thread
      oldParent foreach {parent => treeAdapters foreach(_ deleted (id, parent))}
      def deleteRecurse(id: Long) : Unit = {
        children(id) foreach {child => 
          deleteRecurse(child id)
          treeAdapters foreach(_ deleted(child id, id))
        }
        // EVENTUALLY, react appropriately on unexpected results
        val dataOption = execQuery(dataForId_, id)(_ longOption 1).headOnly
        execUpdate(deleteEntry_, id)
        dataOption foreach(dataId => dataAdapters foreach(_ deleted(dataId)))
      }
      deleteRecurse(id)
    }
    markedDeleted
  }
}

object TreeSqlDB {
  def apply(connection: Connection) = new TreeSqlDB(connection)
  
  def createTables(connection: Connection) : Unit = {
    // The tree is represented by nodes that store their parent but not their children.
    // The tree root must not be deleted, has the ID 0 and parent 0.
    // time should be not NULL iff dataid is not NULL
    // dataid should be 0 for 0-byte TreeEntries
    // UNIQUE (parent, name) is needed in createNewNode()
    execUpdate(connection, """
      CREATE CACHED TABLE TreeEntries (
        id     BIGINT PRIMARY KEY,
        parent BIGINT NOT NULL,
        name   VARCHAR(256) NOT NULL,
        time   BIGINT DEFAULT NULL,
        dataid BIGINT DEFAULT NULL,
        UNIQUE (parent, name)
      );
    """)
    // from http://hsqldb.org/doc/guide/databaseobjects-chapt.html
    // PRIMARY KEY, UNIQUE or FOREIGN key constraints [... create] an index automatically.
    execUpdate(connection, "CREATE INDEX idxParent ON TreeEntries(parent);")
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?);", ROOTID, ROOTID, ROOTNAME)
    // used intermediately when deleting nodes
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, 'recently deleted');", DELETEDROOT, DELETEDROOT)
  }

  def dropTables(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE TreeEntries IF EXISTS;")
  
  protected val constraints = List(
    "ParentReference FOREIGN KEY (parent) REFERENCES TreeEntries(id)",
    "ParentSelfReference CHECK (parent != id OR id < 1)",
    "TimestampIffDataPresent CHECK ((dataid is NULL) = (time is NULL))",
    "ValidId CHECK (id >= -1)",
    "RootNameIsEmpty CHECK ((id != 0) OR (name = ''))"
  )

  protected val externalConstraints = List(
    "DataReference FOREIGN KEY (dataid) REFERENCES DataInfo(id)"
  )
  
  def addConstraints(connection: Connection) : Unit = {
    addInternalConstraints(connection)
    addExternalConstraints(connection)
  }
  
  def addInternalConstraints(connection: Connection) : Unit =
    constraints foreach(constraint => execUpdate(connection, "ALTER TABLE TreeEntries ADD CONSTRAINT " + constraint))
  
  def addExternalConstraints(connection: Connection) : Unit =
    externalConstraints foreach(constraint => execUpdate(connection, "ALTER TABLE TreeEntries ADD CONSTRAINT " + constraint))

  def removeConstraints(connection: Connection) : Unit = {
    constraints foreach(constraint => execUpdate(connection, "ALTER TABLE TreeEntries DROP CONSTRAINT " + constraint.split(" ").head))
    externalConstraints foreach(constraint => execUpdate(connection, "ALTER TABLE TreeEntries DROP CONSTRAINT " + constraint.split(" ").head))
  }
}
