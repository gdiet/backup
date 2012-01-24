// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.sql._
import DataDefinitions.IdAndName

/** Note: Deleted entries are ignored. */
trait SqlForTree { self: SqlCommon =>
  
  private val childrenForIdS = prepare(
      "SELECT id, name FROM TreeEntries WHERE deleted = false AND parent = ?;")
  private val parentForIdS = prepare(
      "SELECT parent FROM TreeEntries WHERE deleted = false AND id = ?;")
  private val nameForIdS = prepare(
      "SELECT name FROM TreeEntries WHERE deleted = false AND id = ?;")
  
  def children(id: Long) : List[IdAndName] =
    execQuery(childrenForIdS, id) {rs => IdAndName(rs long "id", rs string "name")} toList

  def name(id: Long) : Option[String] =
    execQuery(nameForIdS, id) {_ string "name"} headOption
    
  def parent(id: Long) : Option[Long] =
    execQuery(parentForIdS, id) {_ long "parent"} headOption
    
}