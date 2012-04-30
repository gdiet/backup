// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

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
import net.diet_rich.util.Events

trait TreeDB {
  /** @return the tree entry, None if no such node. */
  def entry(id: Long) : Option[TreeEntry]
  /** @return the children, empty if no such node. */
  def children(id: Long) : Iterable[TreeEntry]
  /** @return ID, None if node could not be created. */
  def create(parent: Long, name: String) : Option[Long]
  /** @return true if node was renamed. */
  def rename(id: Long, newName: String) : Boolean
  /** @return true if the new time was set. */
  def setTime(id: Long, newTime: Long) : Boolean
  /** @return true if the new data was set. */
  def setData(id: Long, newTime: Option[Long], newData: Option[Long]) : Boolean
  /** @return true if node was moved. */
  def move(id: Long, newParent: Long) : Boolean
  /** @return true if node was deleted. */
  def deleteWithChildren(id: Long) : Boolean
}

trait TreeDBInternals {
  def move(id: Long, entryGetter: Long => Option[TreeEntry], newParent: Long) : Boolean
  def deleteWithChildren(id: Long, entryGetter: Long => Option[TreeEntry], childrenGetter: Long => Iterable[Long]) : Boolean
  def readEvent : Events[TreeEntry]
  def createEvent : Events[TreeEntry]
  def changeEvent : Events[Long]
  def moveEvent : Events[MoveInformation]
  def deleteEvent : Events[TreeEntry]
}

object TreeDB {
  val ROOTID = 0L
  val ROOTNAME = ""
  val ROOTPATH = ""
  val DELETEDROOT = -1L
  
  def apply(connection: java.sql.Connection, config: StringMap) : TreeDB =
    TreeDBCache(connection, config)
}
