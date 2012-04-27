// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import TreeDB._
import df.IdAndName
import net.diet_rich.util.sql._
import scala.collection.immutable.Iterable
import net.diet_rich.util.Configuration._
import java.sql.Connection
import java.sql.SQLException
import scala.collection.mutable.SynchronizedQueue
import TreeSqlDB._

class TreeSqlDB(protected val connection: Connection)
    extends TreeDB with TreeCacheUpdater with TreeDataUpdater with TreeDBInternals with SqlDBCommon {
  
  protected var treeAdapters = new SynchronizedQueue[TreeCacheUpdateAdapter]
  override def registerUpdateAdapter(adapter: TreeCacheUpdateAdapter) = treeAdapters += adapter

  protected var dataAdapters = new SynchronizedQueue[TreeDataUpdateAdapter]
  override def registerUpdateAdapter(adapter: TreeDataUpdateAdapter) = dataAdapters += adapter
  
  protected val maxEntryId = readAsAtomicLong("SELECT MAX(id) FROM TreeEntries;")
  
  protected val childrenForId_ = idxParent(prepare("SELECT id, name FROM TreeEntries WHERE parent = ?;")) // FIXME dataid and time access?
  protected val nameForId_ = prepare("SELECT name FROM TreeEntries WHERE id = ?;") // FIXME dataid and time access?
  protected val parentForId_ = prepare("SELECT parent FROM TreeEntries WHERE id = ?;") // FIXME dataid and time access?
  protected val dataForId_ = prepare("SELECT dataid FROM TreeEntries WHERE id = ?;") // FIXME dataid and time access?
  protected val addEntry_ = prepare("INSERT INTO TreeEntries (id, parent, name) VALUES (? , ? , ?);")
  protected val renameEntry_ = prepare("UPDATE TreeEntries SET name = ? WHERE id = ?;")
  protected val moveEntry_ = prepare("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
  protected val deleteEntry_ = prepare("DELETE FROM TreeEntries WHERE id = ?;")

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
      case 1 => treeAdapters foreach(_ created (id, name, parent)); Some(id) // EVENTUALLY, call adapters from a separate thread
      case n => throwIllegalUpdateException("Create", n, id)
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); None } // EVENTUALLY, check exception details
  }
  
  override def rename(id: Long, newName: String) : Boolean =
    // This method MUST check that there is no sibling with the same name.
    try { execUpdate(renameEntry_, newName, id) match {
      case 0 => false
      case 1 => treeAdapters foreach(_ renamed (id, newName)); true // EVENTUALLY, call adapters from a separate thread
      case n => throwIllegalUpdateException("Rename", n, id)
    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
  
  override def move(id: Long, newParent: Long) : Boolean =
    parent(id) map(move(id, _, newParent)) getOrElse false
  override def move(id: Long, oldParent: Long, newParent: Long) : Boolean =
    // This method MUST check that there will be no sibling with the same name.
    try { execUpdate(moveEntry_, newParent, id) match {
      case 0 => false
      case 1 => treeAdapters foreach(_ moved (id, oldParent, newParent)); true // EVENTUALLY, call adapters from a separate thread
      case n => throwIllegalUpdateException("Move", n, id)
    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
  
  override def deleteWithChildren(id: Long) : Boolean =
    parent(id) map(deleteWithChildren(id, _)) getOrElse false
  override def deleteWithChildren(id: Long, oldParent: Long) : Boolean = {
    val markedDeleted = try { execUpdate(moveEntry_, DELETEDROOT, id) match {
      case 0 => false
      case 1 => true
      case n => throwIllegalUpdateException("Delete", n, id)
    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
    if (markedDeleted) {
      treeAdapters foreach(_ deleted (id, oldParent)) // EVENTUALLY, call adapters and delete from a separate thread
      def deleteRecurse(id: Long) : Unit = {
        children(id) foreach {child => 
          deleteRecurse(child id)
          treeAdapters foreach(_ deleted(child id, id))
        }
        // EVENTUALLY, react appropriately on unexpected results
        val dataOption = execQuery(dataForId_, id)(_ longOption 1).headOnly // FIXME use cached dataid?
        execUpdate(deleteEntry_, id)
        dataOption foreach(dataId => dataAdapters foreach(_ deleted(dataId)))
      }
      deleteRecurse(id)
    }
    markedDeleted
  }
}

object TreeSqlDB extends SqlDBObjectCommon {
  override val tableName = "TreeEntries"
    
  def createTable(connection: Connection) : Unit = {
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
    execUpdate(connection, "CREATE INDEX idxDataid ON TreeEntries(dataid);")
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?);", ROOTID, ROOTID, ROOTNAME)
    // used intermediately when deleting nodes
    execUpdate(connection, "INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, 'recently deleted');", DELETEDROOT, DELETEDROOT)
  }
  
  // used as index usage markers
  def idxParent[T](t : T) = t
  def idxDataid[T](t : T) = t
  
  override protected val internalConstraints = List(
    "ParentReference FOREIGN KEY (parent) REFERENCES TreeEntries(id)",
    "ParentSelfReference CHECK (parent != id OR id < 1)",
    "TimestampIffDataPresent CHECK ((dataid is NULL) = (time is NULL))",
    "ValidId CHECK (id >= -1)",
    "RootNameIsEmpty CHECK ((id != 0) OR (name = ''))"
  )

  override protected val externalConstraints = List(
    "DataReference FOREIGN KEY (dataid) REFERENCES DataInfo(id)"
  )

  def apply(connection: Connection) = new TreeSqlDB(connection)
}
