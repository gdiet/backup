package dedup

import java.io.File
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import dedup.Database._

import scala.util.Using.resource
import scala.util.chaining._

object Database {
  def dbDir(repo: File): File = new File(repo, "fsdb")

  private def tableDefinitions: Array[String] = {
    s"""|CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  start  BIGINT NOT NULL,
        |  stop   BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id           BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  parentId     BIGINT NOT NULL,
        |  name         VARCHAR(255) NOT NULL,
        |  lastModified BIGINT DEFAULT NULL,
        |  dataId       BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
        |  CONSTRAINT un_TreeEntries UNIQUE (parentId, name),
        |  CONSTRAINT fk_TreeEntries_dataId FOREIGN KEY (dataId) REFERENCES DataEntries(id)
        |);
        |""".stripMargin split ";"
  }

  private val indexDefinitions: Array[String] =
    """|CREATE INDEX DataEntriesStopIdx ON DataEntries(stop);
       |CREATE INDEX DataEntriesHashIdx ON DataEntries(hash);""".stripMargin split ";"

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

  implicit class RichPreparedStatement(val stat: PreparedStatement) extends AnyVal {
    def setLongOption(index: Int, value: Option[Long]): Unit = value match {
      case None => stat.setNull(index, java.sql.Types.BIGINT)
      case Some(t) => stat.setLong(index, t)
    }
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

  private val iTreeEntry = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, lastModified, dataId) VALUES (?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  def addTreeEntry(parent: Long, name: String, lastModified: Option[Long], dataId: Option[Long]): Long = {
    require(lastModified.isEmpty == dataId.isEmpty)
    iTreeEntry.setLong(1, parent)
    iTreeEntry.setString(2, name)
    iTreeEntry.setLongOption(3, lastModified)
    iTreeEntry.setLongOption(4, dataId)
    require(iTreeEntry.executeUpdate() == 1, "Unexpected row count.")
    iTreeEntry.getGeneratedKeys.tap(_.next()).getLong(1)
  }

  override def close(): Unit = connection.close()
}
