package dedup.fs.meta
import dedup.util.sql.ConnectionProvider

object Database {
  private def tableDefinitions: Array[String] =
    """|CREATE SEQUENCE treeEntryIdSeq START WITH 1;
       |CREATE TABLE TreeEntries (
       |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
       |  parent    BIGINT NOT NULL,
       |  name      VARCHAR(255) NOT NULL,
       |  changed   BIGINT DEFAULT NULL,
       |  data      BIGINT DEFAULT NULL,
       |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
       |);
       |INSERT INTO TreeEntries (parent, name) VALUES (0, '');
       |CREATE SEQUENCE treeJournalIdSeq  START WITH 0;""".stripMargin split ";"

  private def indexDefinitions: Array[String] =
    """|DROP   INDEX TreeEntriesIdIdx         IF EXISTS;
       |CREATE INDEX TreeEntriesIdIdx         ON TreeEntries(id);
       |DROP   INDEX TreeEntriesParentIdx     IF EXISTS;
       |CREATE INDEX TreeEntriesParentIdx     ON TreeEntries(parent);
       |DROP   INDEX TreeEntriesParentNameIdx IF EXISTS;
       |CREATE INDEX TreeEntriesParentNameIdx ON TreeEntries(parent, name);""".stripMargin split ";"

  // In first H2 performance checks, prepared statements showed only ~15% better performance than simple statements,
  // so for the KISS approach, plain statements are used.
  def create()(implicit connections: ConnectionProvider): Unit = connections.use { connection =>
    val statement = connection.createStatement()
    tableDefinitions.foreach(statement.execute)
    indexDefinitions.foreach(statement.execute)
  }
}
