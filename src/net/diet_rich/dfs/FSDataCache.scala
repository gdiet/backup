// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import java.util.concurrent.atomic.AtomicLong
import DataDefinitions._

/** Data provided by this cache reflects either the current or a recent state. */
class FSDataCache(db: SqlDB) {

  val settings = db settings
  
  // EVENTUALLY consider synchronization that allows to "unget" a new entry on failure to insert it
  private val nextEntry = new AtomicLong(db.maxEntryID + 1)
  
  /** @return The ID for a path or None if there is no such entry. */
  def get(path: String) : Option[Long] =
    path.split("/").tail
    .foldLeft(Option(0L))((parent, name) => parent flatMap ( get(_, name) ))
    
  /** @return ID of the corresponding child element if any. */
  def get(id: Long, child: String) : Option[Long] =
    db children id find (_.name == child) map (_.id)
  
  /** Create a child element.
   * 
   *  @return ID if element was created, None if element already exists.
   */
  def make(id: Long, childName: String, dataInfo: Option[StoredFileInfo]) : Option[Long] = {
    val childId = nextEntry.getAndIncrement()
    if (db make (childId, id, childName)) Some(childId) else None
  }
  
}