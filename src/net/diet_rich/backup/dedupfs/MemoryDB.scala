// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.dedupfs

import java.util.concurrent.atomic.AtomicLong
import collection.mutable.{HashMap,Queue,SynchronizedMap}

/** in-memory database implementation for testing.
 */
class MemoryDB() extends Database {
  override val writeCache = new HashMap[Long, DBEntry]() with SynchronizedMap[Long, DBEntry]
  override val nextEntryID = new AtomicLong(1)

  // when creating, insert root entry
  private val entryMap = HashMap[Long, DBEntry](0L -> new DBDir(0, "", 0, Nil))

  private def updateDirChildIDs(entry: DBEntry) : DBEntry = entry match {
    case dir: DBDir =>
      dir.copy(childIDs = entryMap.values filter(_.parent == entry.id) map(_.id) toList)
    case other => other
  }
  
  def get(id: Long) : Option[DBEntry] = synchronized { entryMap get id map updateDirChildIDs }

  def get(name: String, parent: Long) : Option[DBEntry] = synchronized {
    entryMap get parent match {
      case dir: DBDir => (dir.childIDs map entryMap.get) .flatten find (_.name == name) map updateDirChildIDs
      case _ => throw new IllegalArgumentException // for a non-dir parent, this method does not make any sense
    }
  }
  
  def writeEntry(id: Long) : Unit = synchronized {
    // entry may not be in write cache because it was queued for writing twice
    writeCache remove id foreach (entryMap += id -> _)
  }
}
