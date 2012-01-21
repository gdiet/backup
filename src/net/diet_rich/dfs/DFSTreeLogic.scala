// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

trait DFSTreeLogic extends DFSTree {

  /** @return The parent path for a path, "" for the root path "". */
  protected final def parent(path: String) =
    if (path == "") "" else path substring (0, path lastIndexOf "/")
  
  /** @return The element name for a path, "" for the root path "". */
  protected final def name(path: String) =
    if (path == "") "" else path substring (1 + path lastIndexOf "/", path length)

  private def makeFromParent(path: String) : Option[Long] =
    getOrMake(parent(path)) flatMap { parent => make(parent, name(path)) }
  
  final def make(path: String) : Option[Long] =
    get (path) match {
      case None => makeFromParent (path)
      case _ => None
    }

  final def getOrMake(path: String) : Option[Long] =
    get (path) orElse makeFromParent (path)

  final def getOrMake(id: Long, child: String) : Option[Long] =
    get(id, child) orElse make(id, child)

}