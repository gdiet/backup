// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.fdfs.old

import net.diet_rich.util.Configuration._
import net.diet_rich.util.Events
import net.diet_rich.util.sql._
import net.diet_rich.fdfs.{SqlDBCommon,SqlDBObjectCommon}
import java.sql.Connection
import com.google.common.cache.LoadingCache
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader

case class TreeEntry(id: Long, parent: Long, name: String, time: Long, dataid: Long)

trait TreeDB {
  /** @return The tree entry, None if no such node. */
  def entry(id: Long) : Option[TreeEntry]
//  /** @return The children, None if no such node or child. */
//  def child(id: Long, name: String) : Option[TreeEntry]
  /** @return The children, empty if no such node. */
  def children(id: Long) : Iterable[TreeEntry]
  /** @return The entry ID. */
  def create(parent: Long, name: String) : Long
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
  val NODATAID = -1L
}

object TreeSqlDB extends SqlDBObjectCommon {
  import TreeDB._
  override val tableName = "TreeEntries"
    
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
        time   BIGINT NOT NULL DEFAULT -1,
        dataid BIGINT NOT NULL DEFAULT %s,
      );
    """ format NODATAID)
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
  
  override protected val debuggingConstraints = List(
    "ParentReference FOREIGN KEY (parent) REFERENCES TreeEntries(id)",
    "ParentSelfReference CHECK ((parent != id) = (id > 0))",
    "TimestampIffDataPresent CHECK ((dataid is NULL) = (time is NULL))",
    "ValidId CHECK (id >= -1)",
    "RootNameIsEmpty CHECK ((id != 0) OR (name = ''))"
  )

}

class TreeSqlDB(protected val connection: Connection) extends SqlDBCommon with TreeDB {
  import java.lang.{Long => JLong}
  import java.util.concurrent.locks.ReentrantReadWriteLock
  import TreeSqlDB._

  case class NodeQueueEntry(parent: Option[Long] = None, name: Option[String] = None, time: Option[Long] = None, dataid: Option[Long] = None) {
    def applyTo(entry: TreeEntry): TreeEntry = {
      entry.copy(
        parent = parent.getOrElse(entry.parent),
        name   = name  .getOrElse(entry.name),
        time   = time  .getOrElse(entry.time),
        dataid = dataid.getOrElse(entry.dataid)
      )
    }
    def applyTo(entry: NodeQueueEntry): NodeQueueEntry = {
      entry.copy(
        parent = if (parent isDefined) parent else entry.parent,
        name   = if (name   isDefined) name   else entry.name,
        time   = if (time   isDefined) time   else entry.time,
        dataid = if (dataid isDefined) dataid else entry.dataid
      )
    }
  }
//  object NodeQueueEntry {
//    def apply() : NodeQueueEntry = NodeQueueEntry(None, None, None, None)
//  }
  
  protected implicit val con = connection

  def children(id: Long) : Iterable[TreeEntry] =
    throw new UnsupportedOperationException
  def create(parent: Long, name: String) : Long =
    throw new UnsupportedOperationException
  def move(id: Long, newParent: Long) : Unit =
    throw new UnsupportedOperationException
  def deleteWithChildren(id: Long) : Unit =
    throw new UnsupportedOperationException
  
    
  protected val cacheSize = 100 // FIXME make configurable
  
//  protected val nodeCacheLock = new ReentrantReadWriteLock
//  protected val nodeCacheReadLock = nodeCacheLock.readLock
//  protected val nodeCacheWriteLock = nodeCacheLock.writeLock
//  def nodeCacheRead[T] (code : => T) : T = {
//    nodeCacheReadLock.lock; try { code } finally { nodeCacheReadLock.unlock }
//  }
//  def nodeCacheWrite[T] (code : => T) : T = {
//    nodeCacheWriteLock.lock; try { code } finally { nodeCacheWriteLock.unlock }
//  }
  
  protected val nodeCache : LoadingCache[JLong, Option[TreeEntry]] =
    CacheBuilder.newBuilder().maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[TreeEntry]] {
      override def load(key: JLong) : Option[TreeEntry] =
        // entry is not in the main cache, so load it
        nodeQueue.synchronized {
          nodeQueueMap.get(key) match {
            // no update for the entry queued
            case None => entryFromSQL(key)
            // "delete entry" queued
            case Some(None) => None
            // "create or update entry" queued
            case Some(Some(diff)) => Some(entryFromSQL(key) match {
              // "create entry" queued
              case None =>
                TreeEntry(key, diff.parent.get, diff.name.get, diff.time.get, diff.dataid.get)
              // "update entry" queued
              case Some(sqlEntry) =>
                diff.applyTo(sqlEntry)
            })
          }
        }
    })

  protected val nodeQueue = collection.mutable.Queue[(Long, Option[NodeQueueEntry])]()
  protected val nodeQueueMap = collection.mutable.Map[Long, Option[NodeQueueEntry]]()
    
  protected val childrenCache : LoadingCache[JLong, Iterable[Long]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Iterable[Long]] {
      override def load(key: JLong) : Iterable[Long] = {
        childrenFromSql(key) // FIXME also use the children queue map
      }
    })
    
  
  protected val queryEntry =
    prepareQuery("SELECT parent, name, time, dataid FROM TreeEntries WHERE id = ?;")
  protected def entryFromSQL(id: Long) : Option[TreeEntry] = {
    queryEntry(id){result =>
      TreeEntry(id, result long 1, result string 2, result long 3, result long 4)
    } headOption
  }
  override def entry(id: Long) : Option[TreeEntry] =
    nodeCache get id

  protected val queryChildren = 
    idxParent(prepareQuery("SELECT id, name, time, dataid FROM TreeEntries WHERE parent = ?;"))
  protected def childrenFromSql(parent: Long) : Iterable[Long] = {
    val entries = queryChildren(parent){result =>
      TreeEntry(result long 1, parent, result string 2, result long 3, result long 4)
    } toList;
    nodeQueue.synchronized {
      // update cache, including the queue diff information
      entries.foreach { entry =>
        nodeQueueMap.get(entry.id) match {
          case None => nodeCache.put(entry.id, Some(entry))
          case Some(None) => /* deleted, don't put into cache */
          case Some(Some(diff)) => nodeCache.put(entry.id, Some(diff.applyTo(entry)))
        }
      }
    }
    entries map (_.id)
  }
    
    
  protected def change(id: Long, diff: NodeQueueEntry) : Unit = {
    nodeQueue.synchronized {
      def enqueue = {
        // enqueue db change
        nodeQueue.enqueue((id, Some(diff)))
        // add entry to queue diff map
        val newDiff = nodeQueueMap.get(id) match {
          // no update for the entry queued yet
          case None => Some(diff)
          // "delete entry" already queued
          case Some(None) => None
          // "create or update entry" already queued
          case Some(Some(previousDiff)) => Some(diff.applyTo(previousDiff))
        }
        nodeQueueMap.put(id, newDiff)
      }
      nodeCache.getIfPresent(id) match {
        case null => enqueue
        case None => /* deleted, nothing to do. */
        case Some(entry) => enqueue; nodeCache.put(id, Some(diff.applyTo(entry)))
      }
    }
  }

  override def rename(id: Long, newName: String) : Unit =
    change(id, NodeQueueEntry(name = Some(newName)))
  override def setTime(id: Long, newTime: Long) : Unit =
    change(id, NodeQueueEntry(time = Some(newTime)))
  override def setData(id: Long, newData: Long) : Unit =
    change(id, NodeQueueEntry(dataid = Some(newData)))
  
  
  protected val renameEntry =
    prepareUpdate("UPDATE TreeEntries SET name = ? WHERE id = ?;")
    
    
//  override def rename(id: Long, newName: String) : Unit = synchronized {
//    // This method MUST check that there is no sibling with the same name.
//    try { renameEntry(newName, id) match {
//      case 0 => false
//      case 1 => changeES emit id; true
//      case n => throwIllegalUpdateException("Rename", n, id)
//    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
//  }
    
//  protected val createES = new EventSource[TreeEntry]
//  override def createEvent : Events[TreeEntry] = createES
//
//  protected val maxEntryId = 
//    readAsAtomicLong("SELECT MAX(id) FROM TreeEntries;")
//  protected val addEntry = 
//    prepareUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (? , ? , ?);")
//  override def create(parent: Long, name: String) : Option[Long] = synchronized {
//    // This method MUST check that the parent exists and there is no child with the same name.
//    val id = maxEntryId incrementAndGet()
//    try { addEntry(id, parent, name) match {
//      case 1 => createES emit TreeEntry(id, parent, name, None, None); Some(id)
//      case n => throwIllegalUpdateException("Create", n, id)
//    } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); None } // EVENTUALLY, check exception details
//  }
//
//
//  protected val changeES = new EventSource[Long]
//  override def changeEvent : Events[Long] = changeES
//
//  protected val changeTime = 
//    prepareUpdate("UPDATE TreeEntries SET time = ? WHERE id = ?;")
//  override def setTime(id: Long, newTime: Long) : Boolean = synchronized {
//    // This method MUST check that there is no sibling with the same name.
//    try { changeTime(newTime, id) match {
//      case 0 => false
//      case 1 => changeES emit id; true
//      case n => throwIllegalUpdateException("Rename", n, id)
//    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
//  }
//  
//
//  protected val changeData = 
//    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?;")
//  override def setData(id: Long, newTime: Option[Long], newData: Option[Long]) : Boolean = synchronized {
//    try { changeData(newTime, newData, id) match {
//      case 0 => false
//      case 1 => changeES emit id; true
//      case n => throwIllegalUpdateException("setData", n, id)
//    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
//  }
//
//  
//  protected val moveES = new EventSource[MoveInformation]
//  override def moveEvent : Events[MoveInformation] = moveES
//
//  protected val moveEntry =
//    prepareUpdate("UPDATE TreeEntries SET parent = ? WHERE id = ?;")
//  override def move(id: Long, newParent: Long) : Boolean = synchronized {
//    move(id, entry, newParent)
//  }
//  override def move(id: Long, entryGetter: Long => Option[TreeEntry], newParent: Long) : Boolean = synchronized {
//    entryGetter(id) map{ oldEntry =>
//      moveNoNotification(id, newParent)(moveES emit MoveInformation(id, oldEntry parent, newParent))
//    } getOrElse false
//  }
//  protected def moveNoNotification(id: Long, newParent: Long)(onSuceess: => Unit) : Boolean =
//    // This method MUST check that there will be no sibling with the same name.
//    try { moveEntry(newParent, id) match {
//      case 0 => false
//      case 1 => onSuceess; true
//      case n => throwIllegalUpdateException("Move", n, id)
//    } } catch { case e: SQLException => false } // EVENTUALLY, check exception details
//
//    
//  protected val deleteES = new EventSource[TreeEntry]
//  override def deleteEvent : Events[TreeEntry] = deleteES
//  
//  protected val deleteEntry =
//    prepareUpdate("DELETE FROM TreeEntries WHERE id = ?;")
//  override def deleteWithChildren(id: Long) : Boolean = synchronized {
//    deleteWithChildren(id, entry, node => children(node) map(_ id))
//  }
//  override def deleteWithChildren(id: Long, entryGetter: Long => Option[TreeEntry], childrenGetter: Long => Iterable[Long]) : Boolean = synchronized {
//    entryGetter(id) map{ oldEntry =>
//      moveNoNotification(id, DELETEDROOT){
//        def deleteRecurse(oldEntry: TreeEntry) : Unit = {
//          childrenGetter(oldEntry id) flatMap (entryGetter(_)) foreach deleteRecurse
//          deleteEntry(oldEntry id) // EVENTUALLY, react appropriately on unexpected results
//          deleteES emit oldEntry
//        }
//        deleteRecurse(oldEntry)
//      }
//    } getOrElse false
//  }
}

