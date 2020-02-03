package dedup

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import org.slf4j.{Logger, LoggerFactory}

import scala.util.Using.resource

object Database {
  def dbDir(repo: File): File = new File(repo, "fsdb")

  // deleted == 0 for regular files, deleted == timestamp for deleted files. NULL does not work with UNIQUE.
  private def tableDefinitions = {
    s"""|CREATE SEQUENCE idSeq START WITH 1;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL,
        |  seq    INTEGER NOT NULL,
        |  length BIGINT NULL,
        |  start  BIGINT NOT NULL,
        |  stop   BIGINT NOT NULL,
        |  hash   BINARY NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id, seq)
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
        |  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
        |);
        |INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', ${root.time});
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

  sealed trait TreeEntry { def parent: Long; def id: Long; def name: String }
  object TreeEntry {
    def apply(parentId: Long, name: String, rs: ResultSet): TreeEntry = rs.opt(_.getLong(3)) match {
      case None => DirEntry(rs.getLong(1), parentId, name, rs.getLong(2))
      case Some(dataId) => FileEntry(rs.getLong(1), parentId, name, rs.getLong(2), dataId)
    }
  }
  case class DirEntry(id: Long, parent: Long, name: String, time: Long) extends TreeEntry
  case class FileEntry(id: Long, parent: Long, name: String, time: Long, dataId: Long) extends TreeEntry

  val root: DirEntry = DirEntry(0, 0, "", now)
}

class Database(connection: Connection) { import Database._
  implicit private val log: Logger = LoggerFactory.getLogger(getClass)

  def startOfFreeData: Long =
    connection.createStatement().executeQuery("SELECT MAX(stop) FROM DataEntries").pipe { rs =>
      rs.maybeNext(_.getLong(1)).getOrElse(0L)
    }

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

  private val qParts = connection.prepareStatement(
    "SELECT start, stop FROM DataEntries WHERE id = ? ORDER BY seq ASCENDING"
  )
  def parts(dataId: Long): Parts = Parts {
    qParts.setLong(1, dataId)
    resource(qParts.executeQuery())(_.seq { rs =>
      val (start, stop) = rs.getLong(1) -> rs.getLong(2)
      assumeLogged(start >= 0, s"start >= 0 ... $start")
      assumeLogged(stop >= start, s"stop >= start ... $stop / $start")
      start -> stop
    })
  }
  def dataSize(dataId: Long): Long = parts(dataId).size

  private val dTreeEntry = connection.prepareStatement(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  def delete(treeEntryId: Long): Boolean = {
    val time = System.currentTimeMillis.pipe { case 0 => 1; case x => x }
    dTreeEntry.setLong(1, time)
    dTreeEntry.setLong(2, treeEntryId)
    (dTreeEntry.executeUpdate() == 1).tap(r => if (!r) log.warn(s"DELETE tree entry $treeEntryId failed."))
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

  private val uTime = connection.prepareStatement(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  def setTime(id: Long, newTime: Long): Boolean = {
    uTime.setLong(1, newTime)
    uTime.setLong(2, id)
    uTime.executeUpdate() == 1
  }

  private val iDir = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  def mkDir(parentId: Long, name: String): Long = {
    iDir.setLong(1, parentId)
    iDir.setString(2, name)
    iDir.setLong(3, now)
    require(iDir.executeUpdate() == 1)
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }

  private val iFile = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, NEXT VALUE FOR idSeq)",
    Statement.RETURN_GENERATED_KEYS
  )
  def mkFile(parentId: Long, name: String, time: Long): Long = {
    iFile.setLong(1, parentId)
    iFile.setString(2, name)
    iFile.setLong(3, time)
    require(iFile.executeUpdate() == 1)
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }

  private val iFileWithDataId = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)"
  )
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Unit = {
    iFileWithDataId.setLong(1, parentId)
    iFileWithDataId.setString(2, name)
    iFileWithDataId.setLong(3, time)
    iFileWithDataId.setLong(4, dataId)
    require(iFileWithDataId.executeUpdate() == 1)
  }

  private val qNextId = connection.prepareStatement("SELECT NEXT VALUE FOR idSeq")
  private def nextId: Long = resource(qNextId.executeQuery())(_.tap(_.next()).getLong(1))

  // Generated keys seem not to be available for sql update, so this is two SQL commands
  def newDataId(id: Long): Long = nextId.tap(setDataId(id, _))

  private val uDataId = connection.prepareStatement(
    "UPDATE TreeEntries SET dataId = ? WHERE id = ?"
  )
  def setDataId(id: Long, dataId: Long): Boolean = {
    uDataId.setLong(1, dataId)
    uDataId.setLong(2, id)
    uDataId.executeUpdate() == 1
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
    "INSERT INTO DataEntries (id, start, stop, hash) VALUES (?, ?, ?, ?)"
  )
  def insertDataEntry(dataId: Long, start: Long, stop: Long, hash: Array[Byte]): Unit = {
    iDataEntry.setLong(1, dataId)
    iDataEntry.setLong(2, start)
    iDataEntry.setLong(3, stop)
    iDataEntry.setBytes(4, hash)
    require(iDataEntry.executeUpdate() == 1)
  }
}
