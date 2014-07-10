package net.diet_rich.dedup.core.sql

import scala.slick.jdbc.{SetParameter, GetResult, StaticQuery}

import net.diet_rich.dedup.core.values.{DataRange, Path, Position, StoreEntry}

object DBUtilities {
  import TableQueries._

  def createTables(hashSize: Int)(implicit session: CurrentSession): Unit =
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
      |CREATE SEQUENCE byteStoreIdSeq START WITH 0;
      |CREATE TABLE ByteStore (
      |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR byteStoreIdSeq),
      |  dataid BIGINT NOT NULL,
      |  start  BIGINT NOT NULL,
      |  fin    BIGINT NOT NULL,
      |  CONSTRAINT pk_ByteStore PRIMARY KEY (id)
      |);
      |CREATE TABLE Settings (
      |  key    VARCHAR(256) NOT NULL,
      |  value  VARCHAR(256) NOT NULL,
      |  CONSTRAINT pk_Settings PRIMARY KEY (key)
      |);
    """.stripMargin execute session

  def recreateIndexes(implicit session: CurrentSession): Unit =
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
    """.stripMargin execute session

  // Settings
  private val allSettingsQuery = StaticQuery.queryNA[(String, String)]("SELECT key, value FROM Settings;")
  def allSettings(implicit session: CurrentSession): Map[String, String] = allSettingsQuery.toMap

  // ByteStore
  private def startOfFreeDataArea(implicit session: CurrentSession) = StaticQuery.queryNA[Position]("SELECT MAX(fin) FROM ByteStore;").firstOption getOrElse Position(0)
  private def dataAreaEnds(implicit session: CurrentSession): List[Position] = StaticQuery.queryNA[Position](
    "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin;"
  ).list
  private def dataAreaStarts(implicit session: CurrentSession): List[Position] = StaticQuery.queryNA[Position](
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start;"
  ).list
  def problemDataAreaOverlaps(implicit session: CurrentSession): List[(StoreEntry, StoreEntry)] = StaticQuery.queryNA[(StoreEntry, StoreEntry)](
    """|SELECT b1.id, b1.dataid, b1.start, b1.fin, b2.id, b2.dataid, b2.start, b2.fin
      |  FROM ByteStore b1 JOIN ByteStore b2 ON
      |    (b1.id != b2.id AND (b1.start = b2.start OR b1.fin = b2.fin)) OR
      |    (b1.start < b2.fin AND b1.fin > b2.fin);""".stripMargin
  ).list
  def freeRangeAtEndOfDataArea(implicit session: CurrentSession): DataRange = DataRange(startOfFreeDataArea, Position(Long.MaxValue))
  def freeRangesInDataArea(implicit session: CurrentSession): List[DataRange] = {
    dataAreaStarts match {
      case Nil => Nil
      case firstArea :: gapStarts =>
        val tail = (dataAreaEnds zip gapStarts).map(DataRange.tupled)
        if (firstArea > Position(0L)) DataRange(Position(0L), firstArea) :: tail else tail
    }
  }
}
