package dedup2

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.util.chaining._
import scala.util.Using.resource

object Database {
  def dbDir(repo: File): File = new File(repo, "fsdb2")

  // deleted == 0 for regular files, deleted == timestamp for deleted files. NULL does not work with UNIQUE.
  private def tableDefinitions = {
    s"""|CREATE SEQUENCE idSeq START WITH 1;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR idSeq),
        |  start  BIGINT NOT NULL,
        |  stop   BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |CREATE TABLE TreeEntries (
        |  id           BIGINT NOT NULL DEFAULT (NEXT VALUE FOR idSeq),
        |  parentId     BIGINT NOT NULL,
        |  name         VARCHAR(255) NOT NULL,
        |  created      BIGINT NOT NULL,
        |  lastModified BIGINT DEFAULT NULL,
        |  deleted      BIGINT NOT NULL DEFAULT 0,
        |  dataId       BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
        |  CONSTRAINT un_TreeEntries UNIQUE (parentId, name, deleted),
        |  CONSTRAINT fk_TreeEntries_dataId FOREIGN KEY (dataId) REFERENCES DataEntries(id),
        |  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
        |);
        |INSERT INTO TreeEntries (id, parentId, name, created) VALUES (0, 0, '', $now);
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
    def maybeNext[T](f: ResultSet => T): Option[T] =
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

  sealed trait TreeEntry { def name: String }
  object TreeEntry {
    def apply(parentId: Long, name: String, rs: ResultSet): TreeEntry = rs.opt(_.getLong(3)) match {
      case None => DirEntry(rs.getLong(1), parentId, name)
      case Some(dataId) => FileEntry(rs.getLong(1), parentId, name, rs.getLong(2), dataId)
    }
  }
  case class DirEntry(id: Long, parent: Long, name: String) extends TreeEntry
  case class FileEntry(id: Long, parent: Long, name: String, lastModified: Long, dataId: Long) extends TreeEntry

  val root = DirEntry(0, 0, "")
}

class Database(connection: Connection) { import Database._

  private val qChild = connection.prepareStatement(
    "SELECT id, lastModified, dataId FROM TreeEntries WHERE parentId = ? AND name = ? AND deleted = 0"
  )
  def child(parentId: Long, name: String): Option[TreeEntry] = {
    qChild.setLong(1, parentId)
    qChild.setString(2, name)
    resource(qChild.executeQuery())(_.maybeNext(TreeEntry(parentId, name, _)))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT id, lastModified, dataId, name FROM TreeEntries WHERE parentId = ? AND deleted = 0"
  )
  def children(parentId: Long): Seq[TreeEntry] = {
    qChildren.setLong(1, parentId)
    resource(qChildren.executeQuery())(_.seq(rs => TreeEntry(parentId, rs.getString(4), rs)))
  }

  private val qStartStop = connection.prepareStatement(
    "SELECT start, stop FROM DataEntries WHERE id = ?"
  )
  def size(dataId: Long): Long = {
    qStartStop.setLong(1, dataId)
    resource(qStartStop.executeQuery())(_.maybeNext(rs => rs.getLong(2) - rs.getLong(1)).getOrElse(0))
  }

}

