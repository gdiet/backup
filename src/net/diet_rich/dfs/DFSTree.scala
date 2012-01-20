// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

trait DFSTree {
  
  /** Create any missing elements in the path, all without data information.
   * 
   *  @return ID if path was created, None if path already exists or missing write permission. */
  def make(path: String) : Option[Long]
  
  /** Create any missing elements in the path, all without data information.
   * 
   *  @return ID or None if missing write permission. */
  def getOrMake(path: String) : Option[Long]

  /** @return The ID for a path or None if there is no such entry. */
  def get(path: String) : Option[Long]
  
  /** Create a child element without data information.
   * 
   *  @return ID if element was created, None if element already exists or missing write permission. */
  def make(id: Long, child: String) : Option[Long]

  /** Create a child element if missing.
   * 
   *  @return ID or None if missing write permission. */
  def getOrMake(id: Long, child: String) : Option[Long]
  
  /** @return ID of the corresponding child element if any. */
  def get(id: Long, child: String) : Option[Long]

  // FIXME name(id), path(id), children(id)
  
}