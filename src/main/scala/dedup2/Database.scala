package dedup2

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

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
        |  time         BIGINT NOT NULL,
        |  deleted      BIGINT NOT NULL DEFAULT 0,
        |  dataId       BIGINT DEFAULT NULL,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
        |  CONSTRAINT un_TreeEntries UNIQUE (parentId, name, deleted),
        |  CONSTRAINT fk_TreeEntries_dataId FOREIGN KEY (dataId) REFERENCES DataEntries(id),
        |  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
        |);
        |INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', ${root.time});
        |INSERT INTO TreeEntries (parentId, name, time) VALUES (0, '1-hallo', 0);
        |INSERT INTO TreeEntries (parentId, name, time) VALUES (0, '2-welt', 0);
        |INSERT INTO TreeEntries (parentId, name, time) VALUES (1, '3-x', 0);
        |INSERT INTO TreeEntries (parentId, name, time) VALUES (1, '4-y', 0);
        |UPDATE TreeEntries SET parentId = 1, name = '3-xx' WHERE id = 3;
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

  sealed trait TreeEntry { def id: Long; def name: String }
  object TreeEntry {
    def apply(parentId: Long, name: String, rs: ResultSet): TreeEntry = rs.opt(_.getLong(3)) match {
      case None => DirEntry(rs.getLong(1), parentId, name, rs.getLong(2))
      case Some(dataId) => FileEntry(rs.getLong(1), parentId, name, rs.getLong(2), dataId)
    }
  }
  case class DirEntry(id: Long, parent: Long, name: String, time: Long) extends TreeEntry
  case class FileEntry(id: Long, parent: Long, name: String, time: Long, dataId: Long) extends TreeEntry

  val root = DirEntry(0, 0, "", now)
}

class Database(connection: Connection) { import Database._

  private val qChild = connection.prepareStatement(
    "SELECT id, time, dataId FROM TreeEntries WHERE parentId = ? AND name = ? AND deleted = 0"
  )
  def child(parentId: Long, name: String): Option[TreeEntry] = {
    qChild.setLong(1, parentId)
    qChild.setString(2, name)
    resource(qChild.executeQuery())(_.maybeNext(TreeEntry(parentId, name, _)))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT id, time, dataId, name FROM TreeEntries WHERE parentId = ? AND deleted = 0"
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

  private val dTreeEntry = connection.prepareStatement(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  def delete(treeEntryId: Long): Boolean = {
    val time = System.currentTimeMillis.pipe { case 0 => 1; case x => x }
    dTreeEntry.setLong(1, time)
    dTreeEntry.setLong(2, treeEntryId)
    dTreeEntry.executeUpdate() == 1
  }

  private val uMoveRename = connection.prepareStatement(
    "UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?"
  )
  def moveRename(id: Long, newParentId: Long, newName: String): Boolean = {
    uMoveRename.setLong(1, newParentId)
    uMoveRename.setString(2, newName)
    uMoveRename.setLong(3, id)
    uMoveRename.executeUpdate() == 1
  }

  private val iDir = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)"
  )
  def mkDir(parentId: Long, name: String): Boolean = {
    iDir.setLong(1, parentId)
    iDir.setString(2, name)
    iDir.setLong(3, now)
    iDir.executeUpdate() == 1
  }

  private val iFile = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)"
  )
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Boolean = {
    iFile.setLong(1, parentId)
    iFile.setString(2, name)
    iFile.setLong(3, time)
    iFile.setLong(4, dataId)
    iFile.executeUpdate() == 1
  }

  private val iDataEntry = connection.prepareStatement(
    "INSERT INTO DataEntries (start, stop, hash) VALUES (0, 0, 0x)",
    Statement.RETURN_GENERATED_KEYS
  )
  def mkDataEntry(): Long = {
    iDataEntry.executeUpdate()
    iDataEntry.getGeneratedKeys.tap(_.next()).getLong(1)
  }
}
