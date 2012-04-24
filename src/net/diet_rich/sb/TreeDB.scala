// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import df.IdAndName
import java.sql.PreparedStatement
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._
import scala.collection.immutable.Iterable
import java.util.concurrent.atomic.AtomicLong
import java.sql.SQLIntegrityConstraintViolationException
import java.sql.SQLException
import scala.collection.mutable.SynchronizedQueue
import java.sql.Connection
import net.diet_rich.util.Configuration._

trait TreeDB {
  /** @return the name, None if no such node. */
  def name(id: Long) : Option[String]
  /** @return the children, empty if no such node. */
  def children(id: Long) : Iterable[IdAndName]
  /** @return the parent ID, None if no such node. */
  def parent(id: Long) : Option[Long]
  /** @return ID, None if node could not be created. */
  def createNewNode(parent: Long, name: String) : Option[Long]
  /** @return true if node was renamed. */
  def rename(id: Long, newName: String) : Boolean
  /** @return true if node was moved. */
  def move(id: Long, newParent: Long) : Boolean
  /** @return true if node was deleted. */
  def deleteWithChildren(id: Long) : Boolean
}

trait TreeDBInternals {
  def move(id: Long, oldParent: Long, newParent: Long) : Boolean
  def deleteWithChildren(id: Long, oldParent: Long) : Boolean
}

object TreeDB {
  val ROOTID = 0L
  val ROOTNAME = ""
  val ROOTPATH = ""
  val DELETEDROOT = -1L
  
  def apply(connection: java.sql.Connection, config: StringMap) : TreeDB =
    TreeDBCache(connection, config)
}
