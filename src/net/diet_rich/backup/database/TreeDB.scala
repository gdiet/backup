package net.diet_rich.backup.database

import net.diet_rich.backup.algorithm._
import net.diet_rich.util.sql._
import java.sql.Connection

trait TreeDB {
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: Long, name: String): Long
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: Long): Option[FullDataInformation]
  /** @throws Exception if the node was not updated correctly. */
  def setData(id: Long, time: Long, dataid: Long): Unit
}


trait BasicTreeDB extends TreeDB {
  implicit def connection: Connection
  
  
  protected val maxEntryId =
    SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM TreeEntries")
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?)")
  override def createAndGetId(parentId: Long, name: String): Long = {
    val id = maxEntryId incrementAndGet()
    addEntry(id, parentId, name) match {
      case 1 => id
      case n => throw new IllegalStateException("Tree: Insert node returned %s rows instead of 1".format(n))
    }
  }
  
  
  protected val queryFullDataInformation = prepareQuery(
    "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo " +
    "ON TreeEntries.dataid = DataInfo.id AND TreeEntries.id = ?"
  )
  override final def fullDataInformation(id: Long): Option[FullDataInformation] =
    queryFullDataInformation(id)(
      q => FullDataInformation(q long 1, q long 2, q long 3, q bytes 4, q long 5)
    ).nextOptionOnly

    
  protected val changeData = 
    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?")
  override def setData(id: Long, time: Long, dataid: Long): Unit =
    changeData(time, dataid, id) match {
      case 1 => None
      case n => throw new IllegalStateException("Tree: Update data returned %s rows instead of 1".format(n))
    }
}
