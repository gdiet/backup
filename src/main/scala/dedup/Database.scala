package dedup

import java.io.File
import java.sql.{Connection, ResultSet}

import dedup.Database._

import scala.util.Using.resource
import scala.util.chaining._

object Database {
  val dbDir: File = new File("./fsdb")

  private def tableDefinitions: Array[String] = {
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id           BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  parentId     BIGINT NOT NULL,
        |  name         VARCHAR(255) NOT NULL,
        |  lastModified BIGINT DEFAULT NULL,
        |  dataId       BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parentId, name) VALUES (-1, '');
        |CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  start  BIGINT NOT NULL,
        |  stop   BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parentId, name) VALUES (0, 'dir');
        |INSERT INTO TreeEntries (parentId, name) VALUES (1, 'sub');
        |INSERT INTO TreeEntries (parentId, name) VALUES (1, 'sub2');
        |INSERT INTO TreeEntries (parentId, name) VALUES (3, 'sub2sub');
        |""".stripMargin split ";" // FIXME remove example data
  }

  private val indexDefinitions: Array[String] =
    """|CREATE INDEX TreeEntriesParentIdx     ON TreeEntries(parentId);
       |CREATE INDEX TreeEntriesParentNameIdx ON TreeEntries(parentId, name);
       |CREATE INDEX DataEntriesStopIdx       ON DataEntries(stop);""".stripMargin split ";"

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

  case class ByParentNameResult(id: Long, lastModified: Option[Long], start: Option[Long], stop: Option[Long])
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
    "SELECT t.id, t.lastModified, d.start, d.stop FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataId = d.id WHERE t.parentId = ? AND t.name = ?"
  )
  def entryByParentAndName(parentId: Long, name: String): Option[ByParentNameResult] = {
    qTreeParentName.setLong(1, parentId)
    qTreeParentName.setString(2, name)
    resource(qTreeParentName.executeQuery())(ByParentNameResult(_))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT name FROM TreeEntries WHERE parentId = ?"
  )
  def children(parentId: Long): Seq[String] = {
    qChildren.setLong(1, parentId)
    resource(qChildren.executeQuery())(r => r.seq(_.getString(1)))
  }

  override def close(): Unit = connection.close()
}
