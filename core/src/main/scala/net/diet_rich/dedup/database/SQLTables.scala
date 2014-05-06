// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.StaticQuery.interpolation

import net.diet_rich.dedup.values._
import net.diet_rich.dedup.util._

object SQLTables {
  import scala.slick.jdbc.GetResult
  import scala.slick.jdbc.SetParameter

  type Session = scala.slick.driver.JdbcDriver#Backend#Session

  implicit val setLongValue = SetParameter((v: LongValue, p) => p setLong v.value)
  implicit val setLongValueOption = SetParameter((v: Option[LongValue], p) => p setLongOption (v map (_ value)))

  implicit val getTreeEntryId = GetResult(r => TreeEntryID(r nextLong))
  implicit val getTimeResult = GetResult(r => Time(r nextLong))
  implicit val getTimeOptionResult = GetResult(r => Time(r nextLongOption))
  implicit val getDataEntryIdOptionResult = GetResult(r => DataEntryID(r nextLongOption))
  implicit val getTreeEntry = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))

  def createTreeTable(implicit session: Session): Unit =
    StaticQuery updateNA """
      |CREATE SEQUENCE treeEntriesIdSeq;
      |CREATE TABLE TreeEntries (
      |  id      BIGINT DEFAULT (NEXT VALUE FOR treeEntriesIdSeq),
      |  parent  BIGINT NULL,
      |  name    VARCHAR(256) NOT NULL,
      |  time    BIGINT NOT NULL DEFAULT 0,
      |  deleted BIGINT DEFAULT NULL,
      |  dataid  BIGINT DEFAULT NULL,
      |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
      |);
    """.stripMargin execute

  def recreateTreeIndexes(implicit session: Session): Unit =
    StaticQuery updateNA """
      |DROP INDEX idxTreeEntriesParent IF EXISTS;
      |DROP INDEX idxTreeEntriesDataid IF EXISTS;
      |DROP INDEX idxTreeEntriesDeleted IF EXISTS;
      |CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
      |CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);
      |CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);
    """.stripMargin execute
}

trait SQLTables {
  import SQLTables._
  val sessions: ThreadSpecific[Session]
  implicit private def dbSession: Session = sessions

  private val treeEntryForIdQuery  = StaticQuery.query[TreeEntryID, TreeEntry]("SELECT (id, parent, name, time, deleted, dataid) FROM TreeEntries WHERE id = ?;")
  private val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
  
  def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id) firstOption
  private def nextTreeEntryId: TreeEntryID = nextTreeEntryIdQuery first

  // FIXME write in one session and/or one thread only!
  // FIXME default values???
  def create(parent: TreeEntryID, name: String, time: Time = Time(0), dataId: Option[DataEntryID] = None): TreeEntryID =
    init (nextTreeEntryId) {
      id => sqlu"INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES ($id, $parent, $name, $time, $dataId);" execute
    }
}
