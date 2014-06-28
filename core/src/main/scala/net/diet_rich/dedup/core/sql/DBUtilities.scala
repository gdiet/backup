package net.diet_rich.dedup.core.sql

import scala.slick.jdbc.StaticQuery

import net.diet_rich.dedup.core.values.Path

object DBUtilities {

  type Database = scala.slick.driver.JdbcDriver#Backend#Database
  type Session = scala.slick.driver.JdbcDriver#Backend#Session

  def createTables(hashSize: Int)(implicit session: Session): Unit =
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
    """.stripMargin execute session

  private val allSettingsQuery = StaticQuery.queryNA[(String, String)]("SELECT * FROM Settings;")

  def allSettings(implicit session: Session): Map[String, String] = allSettingsQuery.toMap
}
