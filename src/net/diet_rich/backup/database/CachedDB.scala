// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

sealed class CachedDB(db: Database) {

  private val parentMap = collection.mutable.HashMap[Long, List[Long]]()
  private val entryMap = collection.mutable.HashMap[Long, Entry]()
  private val entryQueue = collection.mutable.Queue[Long]()

  private val cacheSize = 5000 // EVENTUALLY make a system config
  
  private def addEntry(entry: Entry) = synchronized {
    if (!entryMap.contains(entry.id)) {
      entryMap += entry.id -> entry
      parentMap += entry.parent -> (entry.id :: parentMap.getOrElse(entry.parent, Nil))
      entryQueue.enqueue(entry.id)
      if (entryQueue.length > cacheSize) {
        val removed = entryMap.remove(entryQueue.dequeue).get
        val children = parentMap.remove(removed.parent).get.filterNot(_ == removed.id)
        if (!children.isEmpty) parentMap += removed.parent -> children
      }
    }
  }

  private def deleteEntry(entry: Entry) = synchronized { 
    entryMap.remove(entry.id).foreach { entry =>
      val children = parentMap.remove(entry.parent).get.filterNot(_ == entry.id)
      if (!children.isEmpty) parentMap += entry.parent -> children
    }
  }

  private def entryForName(name: String, parent: Long) = {
    db.entryForName(name, parent)
  }
  
  def entryForPath(path: String, parent: Long = 0) = {
    require(path.startsWith("/"))
    require(!path.endsWith("/"))
    path.split("/").tail.foldLeft[Option[Entry]](Some(new Entry(parent)))((parent, name) =>
      parent.flatMap( parentEntry => entryForName(name, parentEntry.parent) )
    )
  }

}