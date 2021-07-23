package dedup
package db

import java.io.File
import java.sql.{Connection, ResultSet, Statement, Types}
import scala.util.Try
import scala.util.Using.resource

def dbDir(repo: java.io.File) = java.io.File(repo, "fsdb")

def initialize(connection: Connection): Unit = resource(connection.createStatement) { stat =>
  tableDefinitions.foreach(stat.executeUpdate)
  indexDefinitions.foreach(stat.executeUpdate)
}

class Database(connection: Connection) extends util.ClassLogging:
  resource(connection.createStatement) { stat =>
    resource(stat.executeQuery("SELECT value FROM Context WHERE key = 'db version';"))(_.maybeNext(_.getString(1))) match
      case None =>
        log.error(s"No database version found.")
        throw new IllegalStateException("No database version found.")
      case Some(dbVersion) =>
        log.debug(s"Database version: $dbVersion.")
        require(dbVersion == "2", s"Only database version 2 is supported, detected version is $dbVersion.")
  }

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

  private val qParts = connection.prepareStatement(
    "SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC"
  )
  def parts(dataId: DataId): Vector[(Long, Long)] = synchronized {
    qParts.setLong(1, dataId.toLong)
    resource(qParts.executeQuery())(_.seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      assert(start >= 0, s"Start $start must be >= 0.")
      assert(size >= 0, s"Size $size must be >= 0.")
      start -> size
    })
  }

  private val qDataSize = connection.prepareStatement(
    "SELECT length FROM DataEntries WHERE id = ? AND seq = 1"
  )
  def dataSize(dataId: DataId): Long = synchronized {
    resource(qDataSize.tap(_.setLong(1, dataId.toLong)).executeQuery())(_.maybeNext(_.getLong(1))).getOrElse(0)
  }

  private val qDataEntry = connection.prepareStatement(
    "SELECT id FROM DataEntries WHERE hash = ? AND length = ?"
  )
  def dataEntry(hash: Array[Byte], size: Long): Option[DataId] = synchronized {
    qDataEntry.setBytes(1, hash)
    qDataEntry.setLong(2, size)
    resource(qDataEntry.executeQuery())(_.maybeNext(r => DataId(r.getLong(1))))
  }

  def startOfFreeData: Long = synchronized {
    resource(connection.createStatement().executeQuery("SELECT MAX(stop) FROM DataEntries"))(_.maybeNext(_.getLong(1)).getOrElse(0L))
  }

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

  private val iFile = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  /** @return `Some(id)` or [[None]] if a child entry with the same name already exists. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Option[Long] = Try(synchronized {
    iFile.setLong  (1, parentId     )
    iFile.setString(2, name         )
    iFile.setLong  (3, time.toLong  )
    iFile.setLong  (4, dataId.toLong)
    val count = iFile.executeUpdate() // Name conflict triggers SQL exception due to unique constraint.
    if count != 1 then log.warn(s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }).toOption

  private val uParentName = connection.prepareStatement(
    "UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?"
  )
  def update(id: Long, newParentId: Long, newName: String): Boolean = synchronized {
    uParentName.setLong  (1, newParentId)
    uParentName.setString(2, newName    )
    uParentName.setLong  (3, id         )
    uParentName.executeUpdate() == 1
  }

  private val uDataId = connection.prepareStatement(
    "UPDATE TreeEntries SET dataId = ? WHERE id = ?"
  )
  def setDataId(id: Long, dataId: DataId): Unit = synchronized {
    uDataId.setLong(1, dataId.toLong)
    uDataId.setLong(2, id)
    require(uDataId.executeUpdate() == 1, s"setDataId update count not 1 for id $id dataId $dataId")
  }

  private val qNextId = connection.prepareStatement(
    "SELECT NEXT VALUE FOR idSeq"
  )
  def nextId: Long = synchronized {
    resource(qNextId.executeQuery())(_.tap(_.next()).getLong(1))
  }

  // Generated keys seem not to be available for sql update, so this is two SQL commands
  def newDataIdFor(id: Long): DataId = synchronized {
    nextId.pipe(DataId(_)).tap(setDataId(id, _))
  }

  private val iDataEntry = connection.prepareStatement(
    "INSERT INTO DataEntries (id, seq, length, start, stop, hash) VALUES (?, ?, ?, ?, ?, ?)"
  )
  def insertDataEntry(dataId: DataId, seq: Int, length: Long, start: Long, stop: Long, hash: Array[Byte]): Unit = synchronized {
    require(seq > 0, s"seq not positive: $seq")
    iDataEntry.setLong(1, dataId.toLong)
    iDataEntry.setInt(2, seq)
    if (seq == 1) iDataEntry.setLong(3, length) else iDataEntry.setNull(3, Types.BIGINT)
    iDataEntry.setLong(4, start)
    iDataEntry.setLong(5, stop)
    if (seq == 1) iDataEntry.setBytes(6, hash) else iDataEntry.setNull(3, Types.BINARY)
    require(iDataEntry.executeUpdate() == 1, s"insertDataEntry update count not 1 for dataId $dataId")
  }


extension (rs: ResultSet)
  def withNext[T](f: ResultSet => T): T = { require(rs.next()); f(rs) }
  def maybeNext[T](f: ResultSet => T): Option[T] = Option.when(rs.next())(f(rs))
  def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if rs.wasNull then None else Some(t))
  def seq[T](f: ResultSet => T): Vector[T] = Iterator.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.toVector

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
