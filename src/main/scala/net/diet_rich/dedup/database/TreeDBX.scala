// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.AugmentedString
import net.diet_rich.util.vals.Time
import scala.slick.driver.JdbcDriver.backend.Session
import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.StaticQuery.interpolation

case class TreeEntryX(
  id: TreeEntryID, 
  parent: Option[TreeEntryID], 
  name: String, 
  nodeType: NodeType,
  time: Time = Time(0),
  deleted: Option[Time] = None,
  dataid: Option[DataEntryID] = None
)

object TreeDBX {
  import scala.slick.jdbc.GetResult
  import scala.slick.jdbc.SetParameter
  implicit val setTreeEntryId = SetParameter((id: TreeEntryID, p) => p setLong id.value)
  implicit val getTreeEntryId = GetResult(r => TreeEntryID(r nextLong))
  implicit val getTreeEntryIdOption = GetResult(r => TreeEntryID(r nextLongOption))
  implicit val setNodeType = SetParameter((kind: NodeType, p) => p setInt kind.value)
  implicit val getNodeTypeResult = GetResult(r => NodeType(r nextInt))
  implicit val setTime = SetParameter((time: Time, p) => p setLong time.value)
  implicit val getTimeResult = GetResult(r => Time(r nextLong))
  implicit val getTimeOptionResult = GetResult(r => Time(r nextLongOption))
  implicit val setDataEntryIdOption = SetParameter((id: Option[DataEntryID], p) => p setLongOption (id map (_ value)))
  implicit val getDataEntryIdOptionResult = GetResult(r => DataEntryID(r nextLongOption))
  implicit val getTreeEntryX = GetResult(r => TreeEntryX(r <<, r <<, r <<, r <<, r <<, r <<, r <<))
  
  def createTable(implicit session: Session): Unit =
    StaticQuery updateNA """
      CREATE TABLE TreeEntries (
        id      BIGINT PRIMARY KEY,
        parent  BIGINT NULL,
        name    VARCHAR(256) NOT NULL,
        type    INTEGER NOT NULL,
        time    BIGINT NOT NULL DEFAULT 0,
        deleted BIGINT DEFAULT NULL,
        dataid  BIGINT DEFAULT NULL
      );
      CREATE SEQUENCE treeEntriesIdSeq;
    """.normalizeMultiline execute

  def recreateIndexes(implicit session: Session): Unit =
    StaticQuery updateNA """
      DROP INDEX idxTreeEntriesParent IF EXISTS;
      DROP INDEX idxTreeEntriesDataid IF EXISTS;
      DROP INDEX idxTreeEntriesDeleted IF EXISTS;
      CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
      CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);
      CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);
    """.normalizeMultiline execute
}

trait TreeDBX { import TreeDBX._
  implicit def session: Session
  val additionalQueryFilter: String

  private val queryEntry = StaticQuery.query[TreeEntryID, TreeEntryX](
    "SELECT * FROM TreeEntries WHERE id = ?" + additionalQueryFilter
  )
  private val queryChildren = StaticQuery.query[TreeEntryID, TreeEntryX](
    "SELECT * FROM TreeEntries WHERE parent = ?" + additionalQueryFilter
  )
  private val queryChild = StaticQuery.query[(TreeEntryID, String), TreeEntryX](
    "SELECT * FROM TreeEntries WHERE parent = ? AND name = ?" + additionalQueryFilter
  )
  
  private val queryNextId = StaticQuery.queryNA[Long]("SELECT treeEntriesIdSeq.NEXTVAL")
  private def nextId: Long = queryNextId.first

  def entry(id: TreeEntryID): Option[TreeEntryX] = queryEntry(id) firstOption
  def children(parent: TreeEntryID): List[TreeEntryX] = queryChildren(parent) list
  def child(parent: TreeEntryID, name: String): Option[TreeEntryX] = queryChild(parent, name) firstOption
  def createAndGetId(parent: TreeEntryID, name: String, nodeType: NodeType, time: Time = Time(0), dataId: Option[DataEntryID] = None): TreeEntryID = {
    val id = nextId
    val rows = sqlu"INSERT INTO TreeEntries (id, parent, name, type, time, dataid) VALUES ($id, $parent, $name, $nodeType, $time, $dataId);" first;
    TreeEntryID(id)
  }

}
