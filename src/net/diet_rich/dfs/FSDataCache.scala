// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import java.util.concurrent.atomic.AtomicLong
import DataDefinitions._

/** Data provided by this cache reflects either the current or a recent state. */
class FSDataCache(protected val db: SqlDB) extends CacheForTree with CacheForFiles {
  // EVENTUALLY consider synchronization that allows to "unget" a new entry on failure to insert it
  private val nextEntry = new AtomicLong(db.maxEntryID + 1)
  
//  val settings: FSSettings = db.settings
  
  /** Create a child element.
   * 
   *  @return ID if element was created, None if element already exists.
   */
  def make(parentId: Long, childName: String) : Option[Long] = {
    val childId = nextEntry.getAndIncrement()
    if (db createNewNode (childId, parentId, childName)) Some(childId) else None
  }
  
  def contains(print: TimeSizePrint) : Boolean = db contains print

  /** @return The matching data entry ID if any. */
  def fileId(print: TimeSizePrintHash) : Option[Long] = db fileId print
  
}