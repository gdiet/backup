package dedup

import java.io.File
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import dedup.Database._

import scala.util.Using.resource

object Database {
  def dbDir(repo: File): File = new File(repo, "fsdb")

  // deleted == 0 for regular files, deleted == timestamp for deleted files. NULL does not work with UNIQUE.
  private def tableDefinitions = {
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
        |  deleted      BIGINT NOT NULL DEFAULT 0,
        |  lastModified BIGINT DEFAULT NULL,
        |  dataId       BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
        |  CONSTRAINT un_TreeEntries UNIQUE (parentId, name, deleted),
        |  CONSTRAINT fk_TreeEntries_dataId FOREIGN KEY (dataId) REFERENCES DataEntries(id),
        |  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
        |);
        |INSERT INTO TreeEntries (parentId, name) VALUES (0, '');
        |""".stripMargin split ";"
  }

  private def indexDefinitions =
    """|CREATE INDEX DataEntriesStopIdx ON DataEntries(stop);
       |CREATE INDEX DataEntriesHashIdx ON DataEntries(hash);""".stripMargin split ";"

  def initialize(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
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
  def startOfFreeData: Long =
    connection.createStatement().executeQuery("SELECT MAX(stop) FROM DataEntries").pipe { rs =>
      rs.maybe(_.getLong(1)).getOrElse(0L)
    }

  private val qTreeParentName = connection.prepareStatement(
    "SELECT t.id, t.lastModified, d.start, d.stop FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataId = d.id WHERE t.parentId = ? AND t.name = ? AND t.deleted = 0"
  )
  def entryByParentAndName(parentId: Long, name: String): Option[ByParentNameResult] = {
    qTreeParentName.setLong(1, parentId)
    qTreeParentName.setString(2, name)
    resource(qTreeParentName.executeQuery())(ByParentNameResult(_))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT id, name FROM TreeEntries WHERE parentId = ? and deleted = 0"
  )
  def children(parentId: Long): Seq[(Long, String)] = {
    qChildren.setLong(1, parentId)
    resource(qChildren.executeQuery())(_.seq(r => r.getLong(1) -> r.getString(2)))
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

  private val qHash = connection.prepareStatement(
    "SELECT id, start, stop FROM DataEntries WHERE hash = ?"
  )
  def dataEntry(hash: Array[Byte], size: Long): Option[Long] = {
    qHash.setBytes(1, hash)
    resource(qHash.executeQuery())(r => r.seq(_ => (r.getLong(1), r.getLong(2), r.getLong(3)))).collectFirst {
      case (id, start, stop) if stop - start == size => id
    }
  }

  private val iDataEntry = connection.prepareStatement(
    "INSERT INTO DataEntries (start, stop, hash) VALUES (?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  def addDataEntry(start: Long, stop: Long, hash: Array[Byte]): Long = {
    iDataEntry.setLong(1, start)
    iDataEntry.setLong(2, stop)
    iDataEntry.setBytes(3, hash)
    require(iDataEntry.executeUpdate() == 1, "Unexpected row count.")
    iDataEntry.getGeneratedKeys.tap(_.next()).getLong(1)
  }

  override def close(): Unit = connection.close()
}
