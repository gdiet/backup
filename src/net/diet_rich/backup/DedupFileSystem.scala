// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.data.Bytes

class DedupFileSystem (cache: FSDataCache) {

  val settings = cache settings
  
  /** Entry names must be non-empty and must not contain "/". */
  private def isWellformedEntryName(name: String) : Boolean =
    !(name isEmpty) && !(name contains "/")

  /** Sub-paths start but do not end with a slash, and have no consecutive slashes. */
  private def isWellformedSubPath(path: String) : Boolean =
    ((path matches "/.*[^/]") && !(path contains "//"))

  /** Either "" for root or a valid sub-path. */
  private def isWellformedPath(path: String) : Boolean =
    (path equals "") || isWellformedSubPath (path) // root or well formed sub path

  /** @return The parent path for a path, "" for the root path "". */
  private def parent(path: String) =
    if (path == "") "" else path substring (0, path lastIndexOf "/")
  
  /** @return The element name for a path, "" for the root path "". */
  private def name(path: String) = {
    require(isWellformedPath(path))
    if (path == "") "" else path substring (1 + path lastIndexOf "/", path length)
  }
    
  /** Create any missing elements in the path, all without data information.
   * 
   *  @return ID if path was created, None if path already exists or missing write permission.
   */
  def make(path: String) : Option[Long] = {
    require(isWellformedPath(path), "path is not well-formed: " + path)
    cache get path match {
      case None => getOrMake(parent(path)) flatMap (parent => make (parent, name(path)) )
      case _ => None
    }
  }

  /** Create any missing elements in the path, all without data information.
   * 
   *  @return ID or None if missing write permission.
   */
  def getOrMake(path: String) : Option[Long] = {
    require(isWellformedPath(path))
    cache get path orElse {
      getOrMake(parent(path)) flatMap (parent =>
        make(parent, name(path)) orElse child(parent, name(path)) orElse getOrMake(path)
      )
    }
  }

  /** Create a child element.
   * 
   *  @return ID if element was created, None if element already exists or missing write permission.
   */
  def make(id: Long, child: String, dataInfo: Option[StoredFileInfo] = None) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache make (id, child, dataInfo)
  }

  /** @return ID of the corresponding child element if any. */
  def child(id: Long, child: String) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache get (id, child)
  }

  /** @return true if a matching data entry is already stored. */
  def contains(print: TimeSizePrint) : Boolean =
    false // FIXME
  
  /** @return The matching data entry ID if any. */
  def dataId(print: TimeSizePrintHash) : Option[Long] =
    None // FIXME

  /** Store the input data in the repository.
   *  
   *  @return The data ID and the info on the data stored. */
  def store(input: FileDataAccess) : (Long, TimeSizePrintHash) =
    (0, input timeSizePrintHash) // FIXME
    
}