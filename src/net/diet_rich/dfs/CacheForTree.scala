// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.dfs.DataDefinitions.IdAndName

trait CacheForTree {

  protected def db: SqlForTree
  
  /** @return The ID for a path or None if there is no such entry. */
  final def get(path: String) : Option[Long] =
    path.split("/").tail
    .foldLeft(Option(0L))((parent, name) => parent flatMap ( get(_, name) ))
  
  /** @return The ID of the child element if it exists, else None. */
  final def get(id: Long, child: String) : Option[Long] =
    db children id find (_.name == child) map (_.id)
  
  /** @return ID if element was created, None if element already exists 
   *          or missing write permission. */
  def make(parentId: Long, childName: String) : Option[Long]

  /** @return The name of the element if it exists, else None. */
  def name(id: Long) : Option[String] =
    if (id == 0) Some("") else db name id
    
  /** @return The path of the element if it exists, else None. */
  def path(id: Long) : Option[String] =
    if (id == 0) Some("")
    else name(id) flatMap { name =>
      db parent id flatMap (path(_) map (_ + "/" + name))
    }
    
  /** @return The children of the element.
   *          If element does not exists returns the empty list. */
  def children(id: Long) : List[IdAndName] =
    db children id
  
}