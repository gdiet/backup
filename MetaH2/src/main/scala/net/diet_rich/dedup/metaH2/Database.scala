package net.diet_rich.dedup.metaH2

import java.lang.System.{currentTimeMillis => now}

import net.diet_rich.util.Hash
import net.diet_rich.util.sql._

object Database {
  private def tableDefinitions(hashAlgorithm: String): Array[String] = {
    import H2MetaBackend._
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH $rootId;
        |CREATE TABLE TreeEntries (
        |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  parent    BIGINT NOT NULL,
        |  name      VARCHAR(255) NOT NULL,
        |  changed   BIGINT DEFAULT NULL,
        |  data      BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES ($rootsParent, '$rootName');
        |CREATE SEQUENCE treeJournalIdSeq  START WITH 0;
        |CREATE TABLE TreeJournal (
        |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeJournalIdSeq),
        |  treeId    BIGINT NOT NULL,
        |  parent    BIGINT DEFAULT NULL,
        |  name      VARCHAR(256) DEFAULT NULL,
        |  changed   BIGINT DEFAULT NULL,
        |  data      BIGINT DEFAULT NULL,
        |  deleted   BOOLEAN DEFAULT FALSE,
        |  timestamp BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeJournal PRIMARY KEY (id)
        |);
        |INSERT INTO TreeJournal (treeId, parent, name, timestamp) VALUES ($rootId, $rootsParent, '$rootName', $now);
        |CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  length BIGINT NOT NULL,
        |  hash   VARBINARY(${Hash.digestLength(hashAlgorithm)}) NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |CREATE SEQUENCE byteStoreIdSeq START WITH 0;
        |CREATE TABLE ByteStore (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR byteStoreIdSeq),
        |  dataId BIGINT NOT NULL,
        |  start  BIGINT NOT NULL,
        |  fin    BIGINT NOT NULL,
        |  CONSTRAINT pk_ByteStore PRIMARY KEY (id)
        |);
        |CREATE TABLE Settings (
        |  key    VARCHAR(256) NOT NULL,
        |  value  VARCHAR(256) NOT NULL,
        |  CONSTRAINT pk_Settings PRIMARY KEY (key)
        |);""".stripMargin split ";"
  }

  private val indexDefinitions: Array[String] =
    """|DROP   INDEX TreeEntriesIdIdx         IF EXISTS;
       |CREATE INDEX TreeEntriesIdIdx         ON TreeEntries(id);
       |DROP   INDEX TreeEntriesParentIdx     IF EXISTS;
       |CREATE INDEX TreeEntriesParentIdx     ON TreeEntries(parent);
       |DROP   INDEX TreeEntriesParentNameIdx IF EXISTS;
       |CREATE INDEX TreeEntriesParentNameIdx ON TreeEntries(parent, name);
       |DROP   INDEX DataEntriesDuplicatesIdx IF EXISTS;
       |CREATE INDEX DataEntriesDuplicatesIdx ON DataEntries(length, hash);
       |DROP   INDEX ByteStoreDataIdx         IF EXISTS;
       |CREATE INDEX ByteStoreDataIdx         ON ByteStore(dataId);
       |DROP   INDEX ByteStoreStartIdx        IF EXISTS;
       |CREATE INDEX ByteStoreStartIdx        ON ByteStore(start);
       |DROP   INDEX ByteStoreFinIdx          IF EXISTS;
       |CREATE INDEX ByteStoreFinIdx          ON ByteStore(fin);""".stripMargin split ";"

  def create(hashAlgorithm: String, dbSettings: Map[String, String])(implicit con: ConnectionFactory): Unit = {
    tableDefinitions(hashAlgorithm) foreach (update(_).run())
    indexDefinitions foreach (update(_).run())
    // The empty data entry is stored uncompressed
    singleRowUpdate("INSERT INTO DataEntries (length, hash) VALUES (?, ?)")
      .run(0, Hash empty hashAlgorithm)
    insertSettings(dbSettings)
  }

  private def insertSettings(settings: Map[String, String])(implicit cf: ConnectionFactory): Unit = {
    val prepInsertSettings = singleRowUpdate("INSERT INTO Settings (key, value) VALUES (?, ?)")
    settings foreach prepInsertSettings.run
  }

  def replaceSettings(newSettings: Map[String, String])(implicit cf: ConnectionFactory): Unit = {
    update("DELETE FROM Settings").run()
    insertSettings(newSettings)
  }
}
