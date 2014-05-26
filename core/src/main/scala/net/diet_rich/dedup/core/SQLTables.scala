// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.StaticQuery.interpolation

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util._

object SQLTables {
  import scala.slick.jdbc.GetResult
  import scala.slick.jdbc.SetParameter

  type Database = scala.slick.driver.JdbcDriver#Backend#Database
  type Session = scala.slick.driver.JdbcDriver#Backend#Session

  // atomic results
  implicit val _getDataEntryId       = GetResult(r => DataEntryID(r nextLong))
  implicit val _getDataEntryIdOption = GetResult(r => DataEntryID(r nextLongOption))
  implicit val _getHash              = GetResult(r => Hash(r nextBytes))
  implicit val _getPosition          = GetResult(r => Position(r nextLong))
  implicit val _getPrint             = GetResult(r => Print(r nextLong))
  implicit val _getSize              = GetResult(r => Size(r nextLong))
  implicit val _getStoreMethod       = GetResult(r => StoreMethod(r nextInt))
  implicit val _getTimeOption        = GetResult(r => Time(r nextLongOption))
  implicit val _getTreeEntryId       = GetResult(r => TreeEntryID(r nextLong))

  // compound results - order of definition is important
  implicit val _getDataEntry         = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
  implicit val _getDataRange         = GetResult(r => DataRange(r <<, r <<))
  implicit val _getStoreEntry        = GetResult(r => StoreEntry(r <<, r <<, r <<))
  implicit val _getTreeEntry         = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))

  // parameter setters
  implicit val _setHash            = SetParameter((v: Hash, p) => p setBytes v.value)
  implicit val _setIntValue        = SetParameter((v: IntValue, p) => p setInt v.value)
  implicit val _setLongValue       = SetParameter((v: LongValue, p) => p setLong v.value)
  implicit val _setLongValueOption = SetParameter((v: Option[LongValue], p) => p setLongOption (v map (_ value)))

  def createTables(hashSize: Int)(implicit session: Session): Unit = {
    StaticQuery updateNA s"""
      |CREATE SEQUENCE treeEntriesIdSeq START WITH 0;
      |CREATE TABLE TreeEntries (
      |  id      BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntriesIdSeq),
      |  parent  BIGINT NOT NULL,
      |  name    VARCHAR(256) NOT NULL,
      |  changed BIGINT DEFAULT NULL,
      |  dataid  BIGINT DEFAULT NULL,
      |  deleted BIGINT DEFAULT NULL,
      |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
      |);
      |INSERT INTO TreeEntries (parent, name) VALUES (-1, '${Path.ROOTNAME}');
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
      |  CONSTRAINT pk_Settings PRIMARY KEY (key)
      |);
    """.stripMargin execute
  }

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

  val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
  val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
  val selectFromByteStore = "SELECT dataid, index, start, fin FROM ByteStore"
  val selectFromSettings = "SELECT key, value FROM Settings"
}

class SQLTables(database: SQLTables.Database) {
  import SQLTables._
  import java.util.concurrent.Executors.newSingleThreadExecutor
  import scala.concurrent.{Future, ExecutionContext}

  private val sessions = ThreadSpecific(database createSession)

  implicit private val dbWriteContext: ExecutionContext = ExecutionContext fromExecutor newSingleThreadExecutor
  implicit private def dbSession: Session = sessions

  private val writeThread = resultOf(Future(Thread.currentThread))
  def inWriteContext[T] (f: => T): T = if (Thread.currentThread == writeThread) f else resultOf(Future(f))

  // TreeEntries
  private val treeEntryForIdQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
  private val treeChildrenForParentQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
  private val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
  private def nextTreeEntryId: TreeEntryID = nextTreeEntryIdQuery first

  def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id) firstOption
  def treeChildren(parent: TreeEntryID): List[TreeEntry] = treeChildrenForParentQuery(parent) list
  def createTreeEntry(parent: TreeEntryID, name: String, time: Option[Time], dataid: Option[DataEntryID]): TreeEntryID = inWriteContext {
    init(nextTreeEntryId) {
      id => sqlu"INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES ($id, $parent, $name, $time, $dataid);" execute
    }
  }

  // DataEntries
  private val dataEntryForIdQuery = StaticQuery.query[DataEntryID, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
  private val dataEntriesForSizePrintQuery = StaticQuery.query[(Size, Print), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
  private val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Size, Print, Hash), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
  private val nextDataEntryIdQuery = StaticQuery.queryNA[DataEntryID]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")
  private def nextDataEntryId: DataEntryID = nextDataEntryIdQuery first

  def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id) firstOption
  def dataEntries(size: Size, print: Print): List[DataEntry] = dataEntriesForSizePrintQuery(size, print) list
  def dataEntries(size: Size, print: Print, hash: Hash): List[DataEntry] = dataEntriesForSizePrintHashQuery(size, print, hash) list
  def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryID = inWriteContext (
    init(nextDataEntryId) {
      id => sqlu"INSERT INTO DataEntries VALUES ($id, $size, $print, $hash, $method);" execute
    }
  )

  // ByteStore
  def startOfFreeDataArea = StaticQuery.queryNA[Position]("SELECT MAX(fin) FROM ByteStore;").firstOption getOrElse Position(0)
  def dataAreaEnds: List[Position] = StaticQuery.queryNA[Position](
    "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin;"
  ).list
  def dataAreaStarts: List[Position] = StaticQuery.queryNA[Position](
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start;"
  ).list
  lazy val illegalDataAreaOverlapsValue: List[(StoreEntry, StoreEntry)] = StaticQuery.queryNA[(StoreEntry, StoreEntry)](
    "SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin;"
  ).list

  def createByteStoreEntry(dataid: DataEntryID, index: Int, range: DataRange): Unit = inWriteContext (
    sqlu"INSERT INTO ByteStore VALUES ($dataid, $index, ${range.start}, ${range.fin});" execute // TODO can be use range directly here?
  )

  // Settings
  private val allSettingsQuery = StaticQuery.queryNA[(String, String)]("SELECT * FROM Settings;")

  def allSettings: Map[String, String] = allSettingsQuery toMap

  // startup checks
  require(treeEntry(FileSystem.ROOTID) == Some(FileSystem.ROOTENTRY))
}
