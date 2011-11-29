// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

class CachedDB(db: Database) {

  private val cacheSize = 5000 // FIXME make a system config
  
  private val entryMap = collection.mutable.HashMap[Long, Entry]()
  private val entryQueue = collection.mutable.Queue[Long]()
  private val writeCache = collection.mutable.HashMap[Long, Entry]()

  private def addOrChangeEntry(entry: Entry) : Unit = {
    entryMap.synchronized {
      // EVENTUALLY check for duplicate elements in the queue
      entryMap += entry.id -> entry
      writeCache.synchronized { writeCache += entry.id -> entry }
      entryQueue.enqueue(entry.id)
      if (entryQueue.length > cacheSize)
        entryMap.remove(entryQueue.dequeue)
    }
    db.writeEntry(entry.id)
  }

  private def entry(id: Long) : Option[Entry] =
    entryMap.synchronized {
      entryMap.get(id).orElse(writeCache.get(id))
    }.orElse(db.entry(id))

  private def entry(name: String, parent: Long) : Option[Entry] = {
    entryMap.synchronized {
      // get parent if cached
      entryMap.get(parent).orElse(writeCache.get(parent))
    } match {
      // not cached - fetch from database
      case None => db.entry(name, parent)
      // get a list of children and find the first matching child
      case dir: Dir => dir.childIDs.map(entry(_)).flatten.find(_.name == name)
      // parent is not a dir => no child entry
      case _ => None
    }
  }
    
//  private def entry(name: String, parent: Long) : Option[Entry] =
//    // get parent if any and unwrap option
//    entry(parent).flatMap(
//      // get parent entry if a dir and unwrap option
//      _.dir.flatMap(
//        // get a list of children for the parent dir
//        _.childIDs.map(entry(_)).flatten
//        // find the first matching child (result: child option)
//        .find(_.name == name)
//      )
//    )
    
  private def entryForPath(path: String, parent: Long = 0) : Option[Entry] = {
    require(path.startsWith("/"))
    require(!path.endsWith("/"))
    path.split("/").tail.foldLeft[Option[Entry]](entry(parent))((parent, name) =>
      parent.flatMap( parentEntry => entry(name, parentEntry.parent) )
    )
  }

//  private def deleteEntryIfCached(entry: Entry) : Unit = entryMap.synchronized { 
//    entryMap.remove(entry.id)
//  }

}

trait Database {
  def entry(id: Long) : Option[Entry]
  def entry(name: String, parent: Long) : Option[Entry]
  /** queue write from write cache. once written removes entry from write cache. */
  def writeEntry(id: Long) : Unit
}

trait Entry {
  def id: Long
  def name: String
  def parent: Long
  def dir: Option[Dir] = this match { case dir: Dir => Some(dir) ; case _ => None }
  def file: Option[File] = this match { case file: File => Some(file) ; case _ => None }
}

abstract class Dir(val childIDs: List[Long]) extends Entry

abstract class File extends Entry

