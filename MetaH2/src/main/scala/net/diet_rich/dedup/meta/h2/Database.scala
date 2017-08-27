package net.diet_rich.dedup.meta.h2

import net.diet_rich.common.sql.{ConnectionFactory, singleRowUpdate, update}
import net.diet_rich.common.vals.Print
import net.diet_rich.common.{Hash, StringMap}
import net.diet_rich.dedup.meta.TreeEntry.root
import net.diet_rich.dedup.store.StoreMethod

object Database {
  val databaseVersionSetting: (String, String) = "databaseVersion" -> "3.0"
  val metaBackendClassSetting: (String, String) = "metaBackendClass" -> "net.diet_rich.dedup.meta.h2.H2BackendFactory"

  // The tables are designed for create-only usage (no updates) and deletes only when purging to free space.
  // To get the current tree state, a clause like WHERE id IN (SELECT MAX(id) from TreeEntries GROUP BY key);
  // is needed. Unique constraints can not be set for the tree (i.e., for sibling nodes with the same names).
  // To support links, a table like LinkEntries (id, key, parent (-> key on TreeEntries), name,
  // target (-> key on TreeEntries), deleted, timestamp) could be added, where id should feed from the same sequence
  // as TreeEntries.id, so it can be used for history purposes.
  private def tableDefinitions(hashAlgorithm: String): Array[String] =
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE SEQUENCE treeEntryKeySeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  key       BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryKeySeq),
        |  parent    BIGINT NOT NULL,
        |  name      VARCHAR(256) NOT NULL,
        |  changed   BIGINT DEFAULT NULL,
        |  dataId    BIGINT DEFAULT NULL,
        |  deleted   BOOLEAN NOT NULL DEFAULT FALSE,
        |  timestamp BIGINT NOT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES (${root.parent}, '${root.name}', ${System.currentTimeMillis});
        |CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  length BIGINT NOT NULL,
        |  print  BIGINT NOT NULL,
        |  hash   VARBINARY(${Hash digestLength hashAlgorithm}) NOT NULL,
        |  method INTEGER NOT NULL,
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

  private val indexDefinitions: Array[String] =
    """|DROP   INDEX idxTreeEntriesParent IF EXISTS;
       |CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
       |DROP   INDEX idxTreeEntriesKey IF EXISTS;
       |CREATE INDEX idxTreeEntriesKey ON TreeEntries(key);
       |DROP   INDEX idxDataEntriesDuplicates IF EXISTS;
       |CREATE INDEX idxDataEntriesDuplicates ON DataEntries(print, length, hash);
       |DROP   INDEX idxByteStoreData IF EXISTS;
       |CREATE INDEX idxByteStoreData ON ByteStore(dataId);
       |DROP   INDEX idxByteStoreStart IF EXISTS;
       |CREATE INDEX idxByteStoreStart ON ByteStore(start);
       |DROP   INDEX idxByteStoreFin IF EXISTS;
       |CREATE INDEX idxByteStoreFin ON ByteStore(fin);""".stripMargin split ";"

  def create(hashAlgorithm: String, dbSettings: StringMap)(implicit cf: ConnectionFactory): Unit = {
    tableDefinitions(hashAlgorithm) foreach (update(_).run())
    indexDefinitions foreach (update(_).run())
    // The empty data entry is stored uncompressed
    update("INSERT INTO DataEntries (length, print, hash, method) VALUES (?, ?, ?, ?)")
      .run(0, Print.empty, Hash empty hashAlgorithm, StoreMethod.STORE)
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
