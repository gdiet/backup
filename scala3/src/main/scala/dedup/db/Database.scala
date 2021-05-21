package dedup
package db

import java.io.File
import java.sql.{Connection, ResultSet, Statement, Types}
import scala.util.Try
import scala.util.Using.resource

def dbDir(repo: java.io.File) = java.io.File(repo, "fsdb")

def initialize(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
  tableDefinitions.foreach(stat.executeUpdate)
  indexDefinitions.foreach(stat.executeUpdate)
}

class Database(connection: Connection) extends util.ClassLogging {

  private def treeEntry(parentId: Long, name: String, rs: ResultSet): TreeEntry =
    rs.opt(_.getLong(3)) match
      case None         => DirEntry (rs.getLong(1), parentId, name, Time(rs.getLong(2))                )
      case Some(dataId) => FileEntry(rs.getLong(1), parentId, name, Time(rs.getLong(2)), DataId(dataId))
  
  private val qChild = connection.prepareStatement(
    "SELECT id, time, dataId FROM TreeEntries WHERE parentId = ? AND name = ? AND deleted = 0"
  )
  def child(parentId: Long, name: String): Option[TreeEntry] = synchronized {
    qChild.setLong  (1, parentId)
    qChild.setString(2, name    )
    resource(qChild.executeQuery())(_.maybeNext(treeEntry(parentId, name, _)))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT id, time, dataId, name FROM TreeEntries WHERE parentId = ? AND deleted = 0"
  )
  def children(parentId: Long): Seq[TreeEntry] = synchronized {
    qChildren.setLong(1, parentId)
    resource(qChildren.executeQuery())(_.seq(rs => treeEntry(parentId, rs.getString(4), rs)))
  }.filterNot(_.name.isEmpty) // On linux, empty names don't work, and the root node has itself as child...

  private val uTime = connection.prepareStatement(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  def setTime(id: Long, newTime: Long): Unit = synchronized {
    uTime.setLong(1, newTime)
    uTime.setLong(2, id)
    val count = uTime.executeUpdate()
    if count != 1 then log.warn(s"For id $id, setTime update count is $count instead of 1.")
  }

  private val dTreeEntry = connection.prepareStatement(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  def delete(id: Long): Unit = synchronized {
    dTreeEntry.setLong(1, now.nonZero.toLong)
    dTreeEntry.setLong(2, id)
    val count = dTreeEntry.executeUpdate()
    if count != 1 then log.warn(s"For id $id, delete count is $count instead of 1.")
  }

  private val iDir = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS
  )
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = Try(synchronized {
    iDir.setLong  (1, parentId  )
    iDir.setString(2, name      )
    iDir.setLong  (3, now.toLong)
    val count = iDir.executeUpdate() // Name conflict triggers SQL exception due to unique constraint.
    if count != 1 then log.warn(s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }).toOption

  private val iFileWithDataId = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)"
  )
  /** @return false if a child entry with the same name already exists. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Boolean = Try(synchronized {
    iFileWithDataId.setLong  (1, parentId     )
    iFileWithDataId.setString(2, name         )
    iFileWithDataId.setLong  (3, time.toLong  )
    iFileWithDataId.setLong  (4, dataId.toLong)
    val count = iFileWithDataId.executeUpdate() // Name conflict triggers SQL exception due to unique constraint.
    if count != 1 then log.warn(s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
  }).isSuccess

  private val uParentName = connection.prepareStatement(
    "UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?"
  )
  def update(id: Long, newParentId: Long, newName: String): Boolean = synchronized {
    uParentName.setLong  (1, newParentId)
    uParentName.setString(2, newName    )
    uParentName.setLong  (3, id         )
    uParentName.executeUpdate() == 1
  }

  // FIXME merge the two iFile operations
  private val iFile = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, -1)",
    Statement.RETURN_GENERATED_KEYS
  )
  /** Creates a file with dataId -1 if possible.
    * 
    * @return `Some(id)` or [[None]] if a child entry with the same name already exists. */
  def mkFile(parentId: Long, name: String, time: Time): Option[Long] = Try(synchronized {
    iFile.setLong  (1, parentId   )
    iFile.setString(2, name       )
    iFile.setLong  (3, time.toLong)
    val count = iFile.executeUpdate()
    if count != 1 then log.warn(s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }).toOption

}

extension (rs: ResultSet)
  def maybeNext[T](f: ResultSet => T): Option[T] = Option.when(rs.next())(f(rs))
  def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if rs.wasNull then None else Some(t))
  def seq[T](f: ResultSet => T): Vector[T] = LazyList.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.toVector

// deleted == 0 for regular files, deleted == timestamp for deleted files. Why? Because NULL does not work with UNIQUE.
private def tableDefinitions =
  s"""|CREATE TABLE Context (
      |  key   VARCHAR(255) NOT NULL,
      |  value VARCHAR(255) NOT NULL
      |);
      |INSERT INTO Context (key, value) VALUES ('db version', '2');
      |CREATE SEQUENCE idSeq START WITH 1;
      |CREATE TABLE DataEntries (
      |  id     BIGINT NOT NULL,
      |  seq    INTEGER NOT NULL,
      |  length BIGINT NULL,
      |  start  BIGINT NOT NULL,
      |  stop   BIGINT NOT NULL,
      |  hash   BINARY NULL,
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
      |INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', $now);
      |""".stripMargin split ";"

// DataEntriesStopIdx: Find start of free data.
// DataEntriesLengthHashIdx: Find data entries by size & hash.
// TreeEntriesDataIdIdx: Find orphan data entries.
private def indexDefinitions =
  """|CREATE INDEX DataEntriesStopIdx ON DataEntries(stop);
     |CREATE INDEX DataEntriesLengthHashIdx ON DataEntries(length, hash);
     |CREATE INDEX TreeEntriesDataIdIdx ON TreeEntries(dataId);""".stripMargin split ";"
