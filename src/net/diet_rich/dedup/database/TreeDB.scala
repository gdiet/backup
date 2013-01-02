// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

trait TreeDB {
  implicit val connection: WrappedConnection
  
  protected val maxEntryId =
    SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM TreeEntries")
      
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID = {
    val id = maxEntryId incrementAndGet()
    addEntry(id, parentId.value, name) match {
      case 1 => TreeEntryID(id)
      case n => throw new IllegalStateException("Tree: Insert node returned %s rows instead of 1".format(n))
    }
  }
  protected val addEntry = 
    prepareUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?)")

  /** @return The child's entry ID if any. */
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID] =
    queryChild(parent.value, name)(q => TreeEntryID(q long 1)).nextOptionOnly
  protected val queryChild = 
    prepareQuery("SELECT id, time, dataid FROM TreeEntries WHERE parent = ? AND name = ?")
  
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: TreeEntryID): Option[FullDataInformation] =
    queryFullDataInformation(id)(
      q => FullDataInformation(Time(q long 1), Size(q long 2), Print(q long 3), Hash(q bytes 4), DataEntryID(q longOption 5))
    ).nextOptionOnly
  protected val queryFullDataInformation = prepareQuery(
    "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo " +
    "ON TreeEntries.dataid = DataInfo.id AND TreeEntries.id = ?"
  )
  
//  /** @throws Exception if the node was not updated correctly. */
//  def setData(id: TreeEntryID, time: Time, dataid: DataEntryID): Unit
}

object TreeDB {
  def createTable(implicit connection: WrappedConnection) : Unit = {
    execUpdate(net.diet_rich.util.Strings normalizeMultiline """
      CREATE TABLE TreeEntries (
        id     BIGINT PRIMARY KEY,
        parent BIGINT NOT NULL,
        name   VARCHAR(256) NOT NULL,
        time   BIGINT NOT NULL DEFAULT 0,
        dataid BIGINT DEFAULT NULL
      );
    """)
    execUpdate("CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);")
    execUpdate("CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);")
    execUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (0, 0, '');")
  }

  def dropTable(implicit connection: WrappedConnection) : Unit =
    execUpdate("DROP TABLE TreeEntries IF EXISTS;")

}
