// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

import collection.mutable.{HashMap,Map,Queue,SynchronizedMap}

class CachedDB(db: Database) {

  private val cacheSize = 5000 // FIXME make a system config
  
  private val entryMap = HashMap[Long, Entry]()
  private val entryQueue = Queue[Long]()
  private val writeCache = new HashMap[Long, Entry]() with SynchronizedMap[Long, Entry]

  private def addOrChangeEntry(entry: Entry) : Unit = {
    entryMap synchronized {
      cacheEntry (entry)
      writeCache += entry.id -> entry
    }
    db writeEntry entry.id
  }

  private def cacheEntry(entry: Entry) : Entry =
    entryMap synchronized {
      // EVENTUALLY check for duplicate elements in the queue
      entryMap += entry.id -> entry
      entryQueue enqueue entry.id
      if (entryQueue.length > cacheSize) entryMap remove entryQueue.dequeue
      entry
    }
  
  private def get(id: Long) : Option[Entry] =
    entryMap synchronized {
      (entryMap get id) orElse (writeCache get id)
    } orElse (db get id map cacheEntry)

  private def get(name: String, parent: Long) : Option[Entry] = {
    entryMap synchronized {
      // get parent if cached
      (entryMap get parent) orElse (writeCache get parent)
    } match {
      // not cached - fetch from database
      case None => db get (name, parent)
      // get a list of children and find the first matching child
      case dir: Dir => (dir.childIDs map get) .flatten find (_.name == name)
      // parent is not a dir => no child entry
      case _ => None
    }
  }
    
  private def getPath(path: String, parent: Long = 0) : Option[Entry] = {
    require(path startsWith "/")
    require(! (path endsWith "/"))
    (path split "/" tail) .foldLeft (get(parent)) ((parent, name) =>
      parent.flatMap( parentEntry => get(name, parentEntry.parent) )
    )
  }

//  private def deleteEntryIfCached(entry: Entry) : Unit = entryMap.synchronized { 
//    entryMap.remove(entry.id)
//  }

}

trait Database {
  def get(id: Long) : Option[Entry]
  def get(name: String, parent: Long) : Option[Entry]
  /** queue write from write cache. once written removes entry from write cache. synchronize on write cache! */
  def writeEntry(id: Long) : Unit
}

class MemoryDB(writeCache: Map[Long, Entry]) extends Database {
  private val entryMap = HashMap[Long, Entry]()
  
  def get(id: Long) : Option[Entry] = synchronized { entryMap get id }
  def get(name: String, parent: Long) : Option[Entry] = synchronized {
    entryMap get parent match {
      case dir: Dir => (dir.childIDs map entryMap.get) .flatten find (_.name == name)
      case _ => None
    }
  }
  def writeEntry(id: Long) : Unit = synchronized {
    // entry may not be in write cache because it was queued for writing twice
    writeCache remove id foreach (entryMap += id -> _)
  }
}

trait Entry {
  def id: Long
  def name: String
  def parent: Long
  def dir : Option[Dir]  = this match { case dir:  Dir  => Some(dir)  ; case _ => None }
  def file: Option[File] = this match { case file: File => Some(file) ; case _ => None }
}

abstract class Dir(val childIDs: List[Long]) extends Entry

abstract class File extends Entry

