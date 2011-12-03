// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

import java.util.concurrent.atomic.AtomicLong
import collection.mutable.{HashMap,Queue,SynchronizedMap}

class CachedDB(db: Database) {

  private val cacheSize = 5000 // FIXME make a system config
  
  private val entryMap = HashMap[Long, DBEntry]()
  private val entryQueue = Queue[Long]()
  
  // EVENTUALLY check - possibly, needs not be synchronized
  private val nextEntryID = db.nextEntryID
  
  // EVENTUALLY check whether to relax synchronization
  // because we don't need a fully consistent state
  // and only have to assert no lookups fail
  
  private def addOrChangeEntry(entry: DBEntry) : Option[Signal] = {
    entryMap synchronized {
      if (get (entry parent) isEmpty)
        Some(ParentDoesNotExist)
      else if (get (entry name, entry parent) isDefined)
        Some(EntryExists)
      else {
        cacheEntry (entry)
        db.writeCache += entry.id -> entry
        None
      }
    } orElse {
      // writing to database needs not be synchronized
      db writeEntry entry.id
      None
    }
  }

  def mkdir(name: String, parent: String) : Option[Signal] = entryMap synchronized {
    getPath(parent) match {
      case None => Some(ParentDoesNotExist)
      case Some(parentEntry) =>
        addOrChangeEntry(new DBDir(nextEntryID get, name, parentEntry id, Nil)) 
        .orElse { nextEntryID.incrementAndGet ; None }
    }
//    if (getPath (path) isDefined)
//      Some(EntryExists)
    None
  }
  
  // FIXME two methods: add / change
//  private def addOrChangeEntry(entry: DBEntry, trueToChange: Boolean) : Boolean = {
//    val result = entryMap synchronized {
//      if ((entryMap contains entry.id) == trueToChange) {
//        cacheEntry (entry)
//        db.writeCache += entry.id -> entry
//        true
//      } else false
//    }
//    if (result) db writeEntry entry.id
//    result
//  }
//
//  def updateEntry(entry: DBEntry) : Boolean = addOrChangeEntry(entry, true)
//  def addEntry(entry: DBEntry) : Boolean = addOrChangeEntry(entry, false)

  private def cacheEntry(entry: DBEntry) : DBEntry =
    entryMap synchronized {
      // EVENTUALLY check for duplicate elements in the queue
      entryMap += entry.id -> entry
      entryQueue enqueue entry.id
      if (entryQueue.length > cacheSize) entryMap remove entryQueue.dequeue
      entry
    }
  
  private def get(id: Long) : Option[DBEntry] =
    entryMap synchronized {
      println("get: " + id)
      (entryMap get id) orElse (db.writeCache get id)
    } orElse (db get id map cacheEntry)

  private def get(name: String, parent: Long) : Option[DBEntry] = {
    entryMap synchronized {
      println("getName: " + name + " ; " + parent)
      // get parent if cached
      (entryMap get parent) orElse (db.writeCache get parent)
    } match {
      // not cached - fetch from database
      case None => db get (name, parent)
      // get a list of children and find the first matching child
      case dir: DBDir => (dir.childIDs map get) .flatten find (_.name == name)
      // parent is not a dir => no child entry
      case _ => None
    }
  }
    
  def getPath(path: String, parent: Long = 0) : Option[DBEntry] = {
    require((path startsWith "/") || (path equals ""))
    require(! (path endsWith "/"))
    println("getPath: " + path + " ; " + parent)
    (path split "/" tail) .foldLeft (get(parent)) ((parent, name) =>
      parent.flatMap( parentEntry => get(name, parentEntry.parent) )
    )
  }

//  private def deleteEntryIfCached(entry: Entry) : Unit = entryMap.synchronized { 
//    entryMap.remove(entry.id)
//  }

}

/** synchronize all methods! */
trait Database {
  def writeCache : SynchronizedMap[Long, DBEntry]
  def get(id: Long) : Option[DBEntry]
  def get(name: String, parent: Long) : Option[DBEntry]
  /** queue write from write cache. once written removes entry from write cache. */
  def writeEntry(id: Long) : Unit
  def nextEntryID : AtomicLong
}

class MemoryDB() extends Database {
  val writeCache = new HashMap[Long, DBEntry]() with SynchronizedMap[Long, DBEntry]
  val nextEntryID = new AtomicLong(1)

  private val entryMap = HashMap[Long, DBEntry](0L -> new DBDir(0, "", 0, Nil))
  
  def get(id: Long) : Option[DBEntry] = synchronized { entryMap get id }
  def get(name: String, parent: Long) : Option[DBEntry] = synchronized {
    entryMap get parent match {
      case dir: DBDir => (dir.childIDs map entryMap.get) .flatten find (_.name == name)
      case _ => None
    }
  }
  def writeEntry(id: Long) : Unit = synchronized {
    // entry may not be in write cache because it was queued for writing twice
    writeCache remove id foreach (entryMap += id -> _)
  }
}

/** root has id 0 and parent 0 */
trait DBEntry {
  def id: Long
  def name: String
  def parent: Long
  def dir : Option[DBDir]  = this match { case dir:  DBDir  => Some(dir)  ; case _ => None }
  def file: Option[DBFile] = this match { case file: DBFile => Some(file) ; case _ => None }
}

class DBDir(
    val id: Long,
    val name: String,
    val parent: Long,
    val childIDs: List[Long]
  ) extends DBEntry

class DBFile(
    val id: Long,
    val name: String,
    val parent: Long
  ) extends DBEntry
