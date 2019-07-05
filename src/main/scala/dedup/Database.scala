package dedup

import java.sql.{Connection, ResultSet}

import dedup.Database.TreeEntry

import scala.util.Using.resource
import scala.util.chaining._

object Database {
  private def tableDefinitions: Array[String] = {
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  parent    BIGINT NOT NULL,
        |  name      VARCHAR(255) NOT NULL,
        |  changed   BIGINT DEFAULT NULL,
        |  data      BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES (0, 'root');
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL,
        |  length BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);""".stripMargin split ";"
  }

  private val indexDefinitions: Array[String] =
    """|DROP   INDEX TreeEntriesParentIdx     IF EXISTS;
       |CREATE INDEX TreeEntriesParentIdx     ON TreeEntries(parent);
       |DROP   INDEX TreeEntriesParentNameIdx IF EXISTS;
       |CREATE INDEX TreeEntriesParentNameIdx ON TreeEntries(parent, name);
       |DROP   INDEX DataEntriesDuplicatesIdx IF EXISTS;
       |CREATE INDEX DataEntriesDuplicatesIdx ON DataEntries(length, hash);""".stripMargin split ";"

  def initialize(connection: Connection): Unit =
    resource(connection.createStatement()) { stat =>
      tableDefinitions.foreach(stat.executeUpdate)
      indexDefinitions.foreach(stat.executeUpdate)
    }

  private implicit class RichResultSet(val rs: ResultSet) extends AnyVal {
    def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if (rs.wasNull) None else Some(t))
  }

  case class TreeEntry(id: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long])
  object TreeEntry {
    def apply(rs: ResultSet): Option[TreeEntry] =
      if (rs.next()) Some(
        TreeEntry(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.opt(_.getLong(4)), rs.opt(_.getLong(5)))
      )
      else None
  }

  val root = TreeEntry(0, 0, "", None, None)
}
class Database(connection: Connection) {
  private def selectTreeEntry = "SELECT id, parent, name, changed, data FROM TreeEntries"
  private def qTreeId = connection.prepareStatement(s"$selectTreeEntry WHERE id = ?")
  private def qTreeParent = connection.prepareStatement(s"$selectTreeEntry WHERE parent = ?")
  private def qTreeParentName = connection.prepareStatement(s"$selectTreeEntry WHERE parent = ? AND name = ?")

  // SELECT t.id, t.changed, t.data, d.length from TreeEntries t LEFT JOIN DataEntries d on t.data = d.id where t.parent = ? and t.name = ?

  def entryById(id: Long): Option[TreeEntry] = {
    qTreeId.setLong(1, id)
    resource(qTreeId.executeQuery())(TreeEntry(_))
  }
  def entryByParent(parent: Long): Option[TreeEntry] = {
    qTreeParent.setLong(1, parent)
    resource(qTreeParent.executeQuery())(TreeEntry(_))
  }
  def entryByParentName(parent: Long, name: String): Option[TreeEntry] = {
    qTreeParentName.setLong(1, parent)
    qTreeParentName.setString(2, name)
    resource(qTreeParent.executeQuery())(TreeEntry(_))
  }
}
