// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import DataDefinitions.FullFileData
import DataDefinitions.TimeAndData
import DataDefinitions.TimeSize
import net.diet_rich.util.io.InputStream

/**
 * Most checks against illegal arguments depend on assertions and database
 * constraints being enabled. If assertions or database constraints are
 * disabled, it might be possible to create data inconsistencies using
 * illegal values.
 * 
 * The store methods return false if storing fails, e.g. because the node 
 * has been deleted.
 */
trait DFSFiles {
  
  /** Store the input data (if necessary) and link it with the tree entry. */
  def store(id: Long, input: FileDataAccess) : Boolean

  /** Link the file data with the tree entry. */
  def store(id: Long, timeAndData: TimeAndData) : Boolean

  /** @return The properties of the data stored with the node if any. */
  def dataProperties(id: Long) : Option[FullFileData]

  /** @return The time stamp of the node's data if any. */
  def time(id: Long) : Option[Long]

  /** @return The size of the node's data if any. */
  def size(id: Long) : Option[Long]

  /** @return The time stamp and size of the node's data if any. */
  def timeAndSize(id: Long) : Option[TimeSize]
  
  /** @return An input stream of the node's data if any. */
  def read(id: Long) : Option[InputStream]
  
}