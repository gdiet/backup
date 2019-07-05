package dedup

import java.sql.{Connection, ResultSet}

import dedup.Database._

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
        |INSERT INTO TreeEntries (parent, name) VALUES (-1, '');
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL,
        |  length BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES (0, 'dir');
        |INSERT INTO TreeEntries (parent, name) VALUES (1, 'sub');
        |INSERT INTO TreeEntries (parent, name) VALUES (1, 'sub2');
        |INSERT INTO TreeEntries (parent, name) VALUES (3, 'sub2sub');
        |""".stripMargin split ";" // FIXME remove example data
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

  implicit class RichResultSet(val rs: ResultSet) extends AnyVal {
    def opt[T](f: ResultSet => T): Option[T] =
      f(rs).pipe(t => if (rs.wasNull) None else Some(t))
    def maybe[T](f: ResultSet => T): Option[T] =
      if (rs.next()) Some(f(rs)) else None
    def seq[T](f: ResultSet => T): Seq[T] =
      LazyList.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.to(List)
  }

  case class ByParentNameResult(id: Long, changed: Option[Long], data: Option[Long], length: Option[Long])
  object ByParentNameResult {
    def apply(rs: ResultSet): Option[ByParentNameResult] = {
      rs.maybe(rs =>
        ByParentNameResult(rs.getLong(1), rs.opt(_.getLong(2)), rs.opt(_.getLong(3)), rs.opt(_.getLong(4)))
      )
    }
  }

  val byParentNameRoot = ByParentNameResult(0, None, None, None)
}
class Database(connection: Connection) extends AutoCloseable {
  override def toString: String = "db"

  private val qTreeParentName = connection.prepareStatement(
    "SELECT t.id, t.changed, t.data, d.length FROM TreeEntries t LEFT JOIN DataEntries d ON t.data = d.id WHERE t.parent = ? AND t.name = ?"
  )
  def entryByParentAndName(parent: Long, name: String): Option[ByParentNameResult] = {
    qTreeParentName.setLong(1, parent)
    qTreeParentName.setString(2, name)
    resource(qTreeParentName.executeQuery())(ByParentNameResult(_))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT name FROM TreeEntries WHERE parent = ?"
  )
  def children(parent: Long): Seq[String] = {
    qChildren.setLong(1, parent)
    resource(qChildren.executeQuery())(r => r.seq(_.getString(1)))
  }

  override def close(): Unit = connection.close()
}
