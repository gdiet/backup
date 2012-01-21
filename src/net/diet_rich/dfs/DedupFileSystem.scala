// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.data.Bytes
import DataDefinitions._

class DedupFileSystem (cache: FSDataCache) extends DFSTreeLogic with DFSTreeRequirements {

  val settings = cache settings
  
  def get(path: String) : Option[Long] = {
    require(isWellformedPath(path))
    cache get path
  }

  def get(id: Long, child: String) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache get (id, child)
  }

  def make(id: Long, child: String) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache make (id, child)
  }

  /** @return true if a matching data entry is already stored. */
  def contains(print: TimeSizePrint) : Boolean = cache contains print // FIXME make private?
  
  /** @return The matching file entry ID if any. */
  def fileId(print: TimeSizePrintHash) : Option[Long] = cache fileId print // FIXME make private?

  /** Store the input data in the repository.
   *  
   *  @return The data ID and the info on the data stored. */
  def store(input: FileDataAccess) : (Long, TimeSizePrintHash) =
    (0, input timeSizePrintHash) // FIXME remove or make private

//  def store(id: Long, input: FileDataAccess) : StoredFileInfo = {
//    val file, info = if (contains (input.timeSizePrint)) {
//      val info = input.timeSizePrintHash
//      fileId(info) match {
//        case Some(id) => (id, info)
//        case None => store (input)
//      }
//    } else store (input)
//    // FIXME continue
//  }    
    
}