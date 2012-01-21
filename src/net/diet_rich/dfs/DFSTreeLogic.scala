// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.dfs.DataDefinitions.IdAndName

trait DFSTreeLogic extends DFSTree with DFSTreeRequirements {

  protected def cache: FSDataCache
  
  /** @return The parent path for a path, "" for the root path "". */
  private final def parent(path: String) =
    if (path == "") "" else path substring (0, path lastIndexOf "/")
  
  /** @return The element name for a path, "" for the root path "". */
  private final def name(path: String) =
    if (path == "") "" else path substring (1 + path lastIndexOf "/", path length)

  private final def makeFromParent(path: String) : Option[Long] =
    getOrMake(parent(path)) flatMap { parent => make(parent, name(path)) }
  
  // --------------------
  
  final override def make(path: String) : Option[Long] =
    get (path) match {
      case None => makeFromParent (path)
      case _ => None
    }
  
  final override def getOrMake(path: String) : Option[Long] =
    get (path) orElse makeFromParent (path)

  final override def get(path: String) : Option[Long] = {
    require(isWellformedPath(path))
    cache get path
  }

  final override def make(id: Long, child: String) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache make (id, child)
  }

  final override def getOrMake(id: Long, child: String) : Option[Long] =
    get(id, child) orElse make(id, child)

  final override def get(id: Long, child: String) : Option[Long] = {
    require(isWellformedEntryName(child))
    cache get (id, child)
  }

  final override def name(id: Long) : Option[String] =
    cache name id

  final override def path(id: Long) : Option[String] =
    cache path id

  final override def children(id: Long) : List[IdAndName] =
    cache children id
    
}