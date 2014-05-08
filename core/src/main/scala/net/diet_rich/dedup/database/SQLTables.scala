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

  // atomic results
  implicit val _getDataEntryId       = GetResult(r => DataEntryID(r nextLong))
  implicit val _getDataEntryIdOption = GetResult(r => DataEntryID(r nextLongOption))
  implicit val _getHash              = GetResult(r => Hash(r nextBytes))
  implicit val _getPrint             = GetResult(r => Print(r nextLong))
  implicit val _getSize              = GetResult(r => Size(r nextLong))
  implicit val _getStoreMethod       = GetResult(r => StoreMethod(r nextInt))
  implicit val _getTimeOption        = GetResult(r => Time(r nextLongOption))
  implicit val _getTreeEntryId       = GetResult(r => TreeEntryID(r nextLong))

  // compound results
  implicit val _getDataEntry         = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
  implicit val _getTreeEntry         = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))

  // parameter setters
  implicit val _setIntValue        = SetParameter((v: IntValue, p) => p setInt v.value)
  implicit val _setLongValue       = SetParameter((v: LongValue, p) => p setLong v.value)
  implicit val _setLongValueOption = SetParameter((v: Option[LongValue], p) => p setLongOption (v map (_ value)))

  def createTables(hashSize: Int)(implicit session: Session): Unit =
    StaticQuery updateNA s"""
      |CREATE SEQUENCE treeEntriesIdSeq;
      |CREATE TABLE TreeEntries (
      |  id      BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntriesIdSeq),
      |  parent  BIGINT NOT NULL,
      |  name    VARCHAR(256) NOT NULL,
      |  time    BIGINT DEFAULT NULL,
      |  deleted BIGINT DEFAULT NULL,
      |  dataid  BIGINT DEFAULT NULL,
      |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
      |);
      |CREATE SEQUENCE dataEntriesIdSeq;
      |CREATE TABLE DataEntries (
      |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntriesIdSeq),
      |  length BIGINT NOT NULL,
      |  print  BIGINT NOT NULL,
      |  hash   VARBINARY($hashSize) NOT NULL,
      |  method INTEGER DEFAULT 0 NOT NULL,
      |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
      |);
      |CREATE TABLE ByteStore (
      |  dataid BIGINT NOT NULL,
      |  index  INTEGER NOT NULL,
      |  start  BIGINT NOT NULL,
      |  fin    BIGINT NOT NULL
      |);
      |CREATE TABLE Settings (
      |  key    VARCHAR(256) NOT NULL,
      |  value  VARCHAR(256) NOT NULL,
      |  CONSTRAINT pk_Settings PRIMARY KEY (id)
      |);
    """.stripMargin execute

  def recreateIndexes(implicit session: Session): Unit =
    StaticQuery updateNA """
      |DROP INDEX idxTreeEntriesParent IF EXISTS;
      |DROP INDEX idxTreeEntriesDataid IF EXISTS;
      |DROP INDEX idxTreeEntriesDeleted IF EXISTS;
      |CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
      |CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);
      |CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);
      |DROP INDEX idxDataEntriesDuplicates IF EXISTS;
      |DROP INDEX idxDataEntriesLengthPrint IF EXISTS;
      |CREATE INDEX idxDataEntriesDuplicates ON DataEntries(length, print, hash);
      |CREATE INDEX idxDataEntriesLengthPrint ON DataEntries(length, print);
      |DROP INDEX idxByteStoreData IF EXISTS;
      |DROP INDEX idxByteStoreStart IF EXISTS;
      |DROP INDEX idxByteStoreFin IF EXISTS;
      |CREATE INDEX idxByteStoreData ON ByteStore(dataid);
      |CREATE INDEX idxByteStoreStart ON ByteStore(start);
      |CREATE INDEX idxByteStoreFin ON ByteStore(fin);
    """.stripMargin execute
}

trait SQLTables {
  import SQLTables._
  import java.util.concurrent.Executors.newSingleThreadExecutor
  import scala.concurrent.{Future, ExecutionContext}

  val sessions: ThreadSpecific[Session]

  implicit private val dbWriteContext: ExecutionContext = ExecutionContext fromExecutor newSingleThreadExecutor
  implicit private def dbSession: Session = sessions

  // TreeEntries
  private val treeEntryForIdQuery = StaticQuery.query[TreeEntryID, TreeEntry]("SELECT id, parent, name, time, deleted, dataid FROM TreeEntries WHERE id = ?;")
  private val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
  private def nextTreeEntryId: TreeEntryID = nextTreeEntryIdQuery first

  // FIXME first or list?
  def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id) firstOption
  def create(parent: TreeEntryID, name: String, time: Option[Time] = None, dataId: Option[DataEntryID] = None): Future[TreeEntryID] = Future {
    init(nextTreeEntryId) {
      id => sqlu"INSERT INTO TreeEntries (id, parent, name, time, dataid) VALUES ($id, $parent, $name, $time, $dataId);" execute
    }
  }

  // DataEntries
  private val dataEntryForIdQuery = StaticQuery.query[DataEntryID, DataEntry]("SELECT id, length, print, hash, method FROM DataInfo WHERE id = ?;")

  // FIXME first or list?
  def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id) firstOption

  // Settings
  private val allSettingsQuery = StaticQuery.queryNA[(String, String)]("SELECT key, value FROM Settings;")

  def allSettings: Map[String, String] = allSettingsQuery toMap
}
