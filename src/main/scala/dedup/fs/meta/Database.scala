package dedup.fs.meta

import scala.util.chaining.scalaUtilChainingOps

import dedup.util.sql.{ConnectionProvider, RichPreparedStatement, RichResultSet}

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
    statement.execute("INSERT INTO TreeEntries (parent, name) VALUES (4, 'world')")
    statement.execute("INSERT INTO TreeEntries (parent, name, changed, data) VALUES (5, 'file', 0, 0)")
  }

  def children(id: Long)(implicit connections: ConnectionProvider): Seq[String] =
    connections.stat { stat =>
      val resultSet = stat.executeQuery(s"SELECT name FROM TreeEntries WHERE parent = $id")
      Vector.unfold(resultSet)(r => if (r.next()) Some(r.string(1) -> r) else None)
    }

  /** @return id, changed or None if dir, data id or None if dir. */
  def node(parent: Long, name: String)
          (implicit connections: ConnectionProvider): Option[(Long, Option[Long], Option[Long])] =
    connections.con { con =>
      val resultSet = con
        .prepareStatement("SELECT id, changed, data FROM TreeEntries WHERE parent = ? and name = ?")
        .query(parent, name)
      if (resultSet.next())
        Some((resultSet.long(1), resultSet.longOption(2), resultSet.longOption(3)))
          .tap(_ => assert(!resultSet.next(), s"expected no more results for $parent / $name"))
      else None
    }

  def addNode(parent: Long, name: String, changed: Option[Long], data: Option[Long])
             (implicit connections: ConnectionProvider): Boolean =
    node(parent, name).isEmpty && {
      val rows = connections.con {
        _.prepareStatement("INSERT INTO TreeEntries (parent, name, changed, data) VALUES (?, ?, ?, ?)")
         .update(parent, name, changed, data)
      }
      assert(rows == 1, s"insert returned $rows instead of 1")
      true
    }

  def moveRename(id: Long, parent: Long, name: String)
             (implicit connections: ConnectionProvider): Boolean =
    {
      val rows = connections.con {
        _.prepareStatement("UPDATE TreeEntries SET parent = ?, name = ? WHERE id = ?")
         .update(parent, name, id)
      }
      assert(rows < 2, s"update returned $rows instead of 0 or 1")
      rows > 0
    }
}
