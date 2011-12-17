// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

import java.util.concurrent.atomic.AtomicLong
import collection.mutable.{HashMap,Queue,SynchronizedMap}
import net.diet_rich.util.logging.Logged

class CachedDB(db: Database) extends Logged {

  private val cacheSize = 5000 // EVENTUALLY make a system config
  
  private val entryMap = HashMap[Long, DBEntry]()
  private val entryQueue = Queue[Long]()
  
  // EVENTUALLY check - possibly, needs not be synchronized
  private val nextEntryID = db.nextEntryID
  
  // EVENTUALLY check whether to relax synchronization
  // because we don't need a fully consistent state
  // and only have to assert no lookups fail
  
  private def addEntry(entry: DBEntry) : Option[Signal] = {
    debug("addEntry", entry.id, entry.name, entry.getClass.getName.split("\\.").last)
    entryMap synchronized {
      get (entry parent) match {
        case dir: DBDir =>
          if (get (entry name, dir id) isDefined)
            Some(EntryExists)
          else {
            cacheEntry (entry)
            db.writeCache += entry.id -> entry
            None
          }          
        case None => Some(ParentDoesNotExist)
        case _ => Some(ParentIsNotADir)
      }
    } orElse {
      // writing to database needs not be synchronized
      db writeEntry entry.id
      None
    }
  } match { case x => debug("addEntry result", x) ; x }

  def mkdir(name: String, parent: String) : Option[Signal] = entryMap synchronized {
    debug("mkdir", name, parent)
    getPath(parent) match {
      case None => Some(ParentDoesNotExist)
      case Some(parentEntry) =>
        addEntry(new DBDir(nextEntryID get, name, parentEntry id, Nil)) 
        .orElse { nextEntryID.incrementAndGet ; None }
    }
//    if (getPath (path) isDefined)
//      Some(EntryExists)
  } match { case x => debug("mkdir result", x) ; x }
  
  // EVENTUALLY two methods: add / change
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
      debug("get by id", id)
      (entryMap get id) orElse (db.writeCache get id)
    } orElse (db get id map cacheEntry) match { case x => debug("get by id result", x) ; x }

  private def get(name: String, parent: Long) : Option[DBEntry] = {
    debug("get", name, parent)
    entryMap synchronized {
      debug("get entryMap", entryMap)
      // get parent if cached
      (entryMap get parent) orElse (db.writeCache get parent)
    } match {
      // not cached - fetch from database
      case None => debug ("get - none") ; db get (name, parent) map cacheEntry
      // get a list of children and find the first matching child
      case Some(dir: DBDir) => debug ("get - DBDir", dir.childIDs) ; (dir.childIDs map get) .flatten find (_.name == name)
      // parent is not a dir => no child entry
      case x => debug ("get - other", x) ; None
    }
  } match { case x => debug("get result", x) ; x }
    
  def getPath(path: String, parent: Long = 0) : Option[DBEntry] = {
    require((path startsWith "/") || (path equals ""))
    require(! (path endsWith "/"))
    debug("getPath", path, parent)
    (path split "/" tail) .foldLeft (get(parent)) ((parent, name) =>
      parent.flatMap( parentEntry => get(name, parentEntry.parent) )
    )
  } match { case x => debug("getPath result", x) ; x }

//  private def deleteEntryIfCached(entry: Entry) : Unit = entryMap.synchronized { 
//    entryMap.remove(entry.id)
//  }

}

/** synchronize all methods! */
trait Database {
  val writeCache : SynchronizedMap[Long, DBEntry]
  val nextEntryID : AtomicLong
  def get(id: Long) : Option[DBEntry]
  def get(name: String, parent: Long) : Option[DBEntry]
  /** queue write from write cache. once written removes entry from write cache. */
  def writeEntry(id: Long) : Unit
}

/** root has id 0, name "", and parent 0 */
trait DBEntry {
  def id: Long
  def name: String
  def parent: Long
  def dir : Option[DBDir]  = this match { case dir:  DBDir  => Some(dir)  ; case _ => None }
  def file: Option[DBFile] = this match { case file: DBFile => Some(file) ; case _ => None }
}

case class DBDir(
  val id: Long,
  val name: String,
  val parent: Long,
  val childIDs: List[Long]
) extends DBEntry

case class DBFile(
  val id: Long,
  val name: String,
  val parent: Long
) extends DBEntry
