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

trait TreeDB {
  /** @return the name, None if no such node. */
  def name(id: Long) : Option[String]
  /** @return the children, empty if no such node. */
  def children(id: Long) : Iterable[IdAndName]
  /** @return the parent ID, None if no such node. */
  def parent(id: Long) : Option[Long]
  /** @return ID, None if node could not be created. */
  def createNewNode(parent: Long, name: String) : Option[Long]
}

trait TreeCacheUpdater {
  def registerUpdateAdapter(adapter: TreeCacheUpdateAdapter)
}

trait TreeCacheUpdateAdapter {
  def created(id: Long, name: String, parent: Long)
}

object TreeDB {
  val ROOTID = 0L
  val ROOTNAME = ""
  val ROOTPATH = ""
  
  def apply(connection: java.sql.Connection) : TreeDB with TreeCacheUpdater = new TreeDB with TreeCacheUpdater {
    var updateAdapters = new SynchronizedQueue[TreeCacheUpdateAdapter]
    def registerUpdateAdapter(adapter: TreeCacheUpdateAdapter) = updateAdapters += adapter
    
    def prepare(statement: String) : ScalaThreadLocal[PreparedStatement] =
      ScalaThreadLocal(connection prepareStatement statement, statement)
    val maxEntryId: AtomicLong = new AtomicLong(
      execQuery(connection, "SELECT MAX ( id ) AS id FROM TreeEntries;")(_ long 1 ) headOnly
    )
    val childrenForId_ = prepare("SELECT id, name FROM TreeEntries WHERE parent = ?;")
    val nameForId_ = prepare("SELECT name FROM TreeEntries WHERE id = ?;")
    val parentForId_ = prepare("SELECT parent FROM TreeEntries WHERE id = ?;")
    val addEntry_ = prepare("INSERT INTO TreeEntries (id, parent, name) VALUES ( ? , ? , ? );")
    
    override def name(id: Long) : Option[String] =
      execQuery(nameForId_, id)(_ string 2) headOption
    override def children(id: Long) : Iterable[IdAndName] =
      execQuery(childrenForId_, id)(result => IdAndName(result long 1, result string 2)) toList
    override def parent(id: Long) : Option[Long] =
      execQuery(parentForId_, id)(_ long 1) headOption
      
    override def createNewNode(parent: Long, name: String) : Option[Long] = {
      // This method MUST check that the parent exists and there is no child with the same name.
      val id = maxEntryId incrementAndGet()
      try { execUpdate(addEntry_, id, parent, name) match {
        // EVENTUALLY, the update adapters should be called from a separate thread
        case 1 => updateAdapters foreach(_ created (id, name, parent)); Some(id)
        case n => throw new IllegalStateException("Unexpected %s times update for id %s" format(n, id))
      } } catch { case e: SQLException => maxEntryId compareAndSet(id, id-1); None }
    }
      
  }
}