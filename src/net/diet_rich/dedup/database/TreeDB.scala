// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

case class TreeEntry(id: TreeEntryID, name: String, time: Time, dataid: DataEntryID)

trait TreeDB {
  implicit val connection: WrappedConnection
  
  private val maxEntryId =
    SqlDBUtil.readAsAtomicLong("SELECT MAX(id) FROM TreeEntries")
  
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: TreeEntryID, name: String): TreeEntryID = {
    val id = maxEntryId incrementAndGet()
    addEntry(id, parentId.value, name)
    TreeEntryID(id)
  }
  protected val addEntry = 
    prepareSingleRowUpdate("INSERT INTO TreeEntries (id, parent, name) VALUES (?, ?, ?)")

  /** @return The child entry if any. */
  def child(parent: TreeEntryID, name: String): Option[TreeEntry] =
    queryChild(parent.value, name)(
      r => TreeEntry(TreeEntryID(r long 1), name, Time(r long 2), DataEntryID(r long 3))
    ).nextOptionOnly
  protected val queryChild = 
    prepareQuery("SELECT id, time, dataid FROM TreeEntries WHERE parent = ? AND name = ?")

  /** @return The children, empty if no such node. */
  def children(parent: TreeEntryID): Iterable[TreeEntry] =
    queryChildren(parent.value)(
      r => TreeEntry(TreeEntryID(r long 1), r string 2, Time(r long 3), DataEntryID(r long 4))
    ).toSeq
  protected val queryChildren =
    prepareQuery("SELECT id, name, time, dataid FROM TreeEntries WHERE parent = ?")
    
  /** @return The node's complete data information if any. */
  def fullDataInformation(id: TreeEntryID): Option[FullDataInformation] =
    queryFullDataInformation(id)(
      q => FullDataInformation(Time(q long 1), Size(q long 2), Print(q long 3), Hash(q bytes 4), DataEntryID(q longOption 5))
    ).nextOptionOnly
  protected val queryFullDataInformation = prepareQuery(
    "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo " +
    "ON TreeEntries.dataid = DataInfo.id AND TreeEntries.id = ?"
  )
  
  /** @throws Exception if the node was not updated correctly. */
  def setData(id: TreeEntryID, time: Time, dataid: Option[DataEntryID]): Unit =
    changeData(time.value, dataid.map(_.value), id.value) match {
      case 1 => Unit
      case n => throw new IllegalStateException(s"Tree: Update node $id returned $n rows instead of 1")
    }
  protected val changeData = 
    prepareUpdate("UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?;")
}

trait TreeDBUtils { self: TreeDB => import TreeDB._
  def childId(parent: TreeEntryID, name: String): Option[TreeEntryID] =
    child(parent, name).map(_.id)
  
  /** @return The entry ID. Missing path elements are created on the fly. */
  def getOrMake(path: Path): TreeEntryID = if (path == ROOTPATH) ROOTID else {
    assume(path.value.startsWith(SEPARATOR), s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value.split(SEPARATOR).drop(1)
    parts.foldLeft(ROOTID) {(node, childName) =>
      val childOption = children(node).find(_.name == childName)
      childOption map(_.id) getOrElse createAndGetId(node, childName)
    }
  }

  /** @return The entry or None if no such entry. */
  def entry(path: Path): Option[TreeEntryID] = if (path == ROOTPATH) Some(ROOTID) else {
    assume(path.value.startsWith(SEPARATOR), s"Path <$path> is not root and does not start with '$SEPARATOR'")
    val parts = path.value.split(SEPARATOR).drop(1)
    parts.foldLeft(Option(ROOTID)) {(node, childName) =>
      node.flatMap(children(_).find(_.name == childName)).map(_.id)
    }
  }
}

object TreeDB {
  val ROOTID = TreeEntryID(0L)
  val ROOTPATH = Path("")
  val SEPARATOR = "/"
    
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