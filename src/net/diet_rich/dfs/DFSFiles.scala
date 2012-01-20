// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import DataDefinitions.FullFileData
import DataDefinitions.TimeAndData

trait DFSFiles {
  
  /** Store the input data (if necessary) and link it with the tree entry.
   *  If the tree entry is marked deleted, it stays deleted.
   *  If the tree entry ID does not exist, the behavior of the method is not specified. */
  def store(id: Long, input: FileDataAccess) : Unit

  /** Link the file data with the tree entry.
   *  If the tree entry is marked deleted, it stays deleted.
   *  If the tree entry ID or the data ID does not exist, the behavior of the method is not specified. */
  def store(id: Long, timeAndData: TimeAndData) : Unit

  def getData(id: Long) : FullFileData
  
}