package dedup.fs.meta

import dedup.util.sql.ConnectionProvider
import dedup.util.sql.RichResultSet
import scala.util.chaining.scalaUtilChainingOps

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
  def create()(implicit connections: ConnectionProvider): Unit = connections.stat { statement =>
    tableDefinitions.foreach(statement.execute)
    indexDefinitions.foreach(statement.execute)
    // FIXME eventually remove example data
    statement.execute("INSERT INTO TreeEntries (parent, name) VALUES (1, 'hallo')")
    statement.execute("INSERT INTO TreeEntries (parent, name) VALUES (2, 'welt')")
    statement.execute("INSERT INTO TreeEntries (parent, name) VALUES (1, 'hello')")
    statement.execute("INSERT INTO TreeEntries (parent, name) VALUES (4, 'welt')")
  }

  def nodeByParentAndName(parent: Long, name: String)
                         (implicit connections: ConnectionProvider): Option[(Long, Option[Long], Option[Long])] =
    connections.con { con =>
      val prep = con.prepareStatement(
        "SELECT id, changed, data FROM TreeEntries WHERE parent = ? and name = ?"
      )
      prep.setLong(1, parent)
      prep.setString(2, name)
      val resultSet = prep.executeQuery()
      if (resultSet.next()) {
        Some((resultSet.long(1), resultSet.longOption(2), resultSet.longOption(3))).tap(
          _ =>
            assert(
              !resultSet.next(),
              s"expected no more results for $parent / $name"
          )
        )
      } else None
    }
}
