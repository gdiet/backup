package net.diet_rich.dedupfs.metadata

import java.sql.Connection

import net.diet_rich.common._

import TreeEntry.root

object Database {
  def create(hashAlgorithm: String)(implicit connection: Connection): Unit =
    tableDefinitions(hashAlgorithm) foreach { ddl => sql update ddl }

  def tableDefinitions(hashAlgorithm: String): Array[String] =
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id      BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  parent  BIGINT NOT NULL,
        |  name    VARCHAR(256) NOT NULL,
        |  changed BIGINT DEFAULT NULL,
        |  dataid  BIGINT DEFAULT NULL,
        |  deleted BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES (${root.parent}, '${root.name}');
        |CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  length BIGINT NOT NULL,
        |  print  BIGINT NOT NULL,
        |  hash   VARBINARY(${Hash digestLength hashAlgorithm}) NOT NULL,
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
        |);""".stripMargin split ";"
}
