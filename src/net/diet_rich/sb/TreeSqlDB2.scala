// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import TreeDB._
import df._
import net.diet_rich.util.sql._
import scala.collection.immutable.Iterable
import net.diet_rich.util.Configuration._
import java.sql.Connection
import java.sql.SQLException
import scala.collection.mutable.SynchronizedQueue
import TreeSqlDB2._
import net.diet_rich.util.EventSource
import net.diet_rich.util.Events
import net.diet_rich.util.ScalaThreadLocal

case class TreeEntry(id: Long, parent: Long, name: String, time: Option[Long], dataid: Option[Long])
case class MoveInformation(id: Long, oldParent: Long, newParent: Long)

class TreeSqlDB2(protected val connection: Connection) extends SqlDBCommon with TreeDB2 with TreeDBInternals2 {
  // the public methods are synchronized to avoid race conditions when updating the caches
  
  protected val readES = new EventSource[TreeEntry]
  override def readEvent : Events[TreeEntry] = readES

  protected val queryEntry = 
    prepareQuery("SELECT parent, name, time, dataid FROM TreeEntries WHERE id = ?;")
  override def entry(id: Long) : Option[TreeEntry] = synchronized {
    queryEntry(id){result =>
      readES.emit(
        TreeEntry(id, result long 1, result string 2, result longOption 3, result longOption 4)
      )
    } headOption
  }
  
  protected val queryChildren = 
    idxParent(prepareQuery("SELECT id, name, time, dataid FROM TreeEntries WHERE parent = ?;"))
  override def children(id: Long) : Iterable[TreeEntry] = synchronized {
    queryChildren(id){result =>
      readES.emit(
        TreeEntry(result long 1, id, result string 2, result longOption 3, result longOption 4)
      )
    } toList
  }

    
  protected val createES = new EventSource[TreeEntry]
  override def createEvent : Events[TreeEntry] = createES

  protected val maxEntryId = 
    readAsAtomicLong("SELECT MAX(id) FROM TreeEntries;")
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (? , ? , ?);")
  override def create(parent: Long, name: String) : Option[Long] = synchronized {
    // This method MUST check that the parent exists and there is no child with the same name.
    val id = maxEntryId incrementAndGet()
    try { addEntry(id, parent, name) match {
      case 1 => createES emit TreeEntry(id, parent, name, None, None); Some(id)
      case n => throwIllegalUpdateException("Create", n, id)
    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); None } // EVENTUALLY, check exception details
  }


  protected val changeES = new EventSource[Long]
  override def changeEvent : Events[Long] = changeES

  protected val renameEntry = 
    prepareUpdate("UPDATE TreeEntries SET name = ? WHERE id = ?;")
  override def rename(id: Long, newName: String) : Boolean = synchronized {
    // This method MUST check that there is no sibling with the same name.
    try { renameEntry(newName, id) match {
      case 0 => false
      case 1 => changeES emit id; true
      case n => throwIllegalUpdateException("Rename", n, id)
    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
  }

    
  protected val moveES = new EventSource[MoveInformation]
  override def moveEvent : Events[MoveInformation] = moveES

  protected val moveEntry =
    prepareUpdate("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
  override def move(id: Long, newParent: Long) : Boolean = synchronized {
    move(id, entry, newParent)
  }
  override def move(id: Long, entryGetter: Long => Option[TreeEntry], newParent: Long) : Boolean = synchronized {
    moveNoNotification(id, entryGetter, newParent) map {
      moveES emit MoveInformation(id, _, newParent)
    } isDefined
  }
  protected def moveNoNotification(id: Long, entryGetter: Long => Option[TreeEntry], newParent: Long) : Option[Long] =
    // This method MUST check that there will be no sibling with the same name.
    entryGetter(id) flatMap{ oldEntry =>
      try { moveEntry(newParent, id) match {
        case 0 => None
        case 1 => Some(oldEntry.parent)
        case n => throwIllegalUpdateException("Move", n, id)
      } } catch { case e: SQLException => None } // EVENTUALLY, check exception details
    }

  
  protected val deleteES = new EventSource[TreeEntry]
  override def deleteEvent : Events[TreeEntry] = deleteES
  
  protected val deleteEntry =
    prepareUpdate("DELETE FROM TreeEntries WHERE id = ?;")
  override def deleteWithChildren(id: Long) : Boolean = synchronized {
    deleteWithChildren(id, entry, node => children(node) map(_ id))
  }
  override def deleteWithChildren(id: Long, entryGetter: Long => Option[TreeEntry], childrenGetter: Long => Iterable[Long]) : Boolean = synchronized {
    entryGetter(id) flatMap{ originalEntry =>
      moveNoNotification(id, entryGetter, DELETEDROOT) map { dontcare =>
        def deleteRecurse(id: Long) : Unit = {
          deleteES emit originalEntry
          childrenGetter(id) foreach {child => deleteRecurse(child)}
          deleteEntry(id) // EVENTUALLY, react appropriately on unexpected results
        }
        deleteRecurse(id)
      }
    } isDefined
  }
}

object TreeSqlDB2 extends SqlDBObjectCommon {
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

  def apply(connection: Connection) = new TreeSqlDB2(connection)
}
