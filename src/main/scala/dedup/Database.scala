package dedup

import java.io.File
import java.lang.System.{currentTimeMillis => now}
import java.sql.{Connection, ResultSet, Statement, Types}

import org.slf4j.{Logger, LoggerFactory}

import scala.util.Using.resource

object Database {
  implicit private val log: Logger = LoggerFactory.getLogger("dedup.DBase")

  def dbDir(repo: File): File = new File(repo, "fsdb")

  // deleted == 0 for regular files, deleted == timestamp for deleted files. NULL does not work with UNIQUE.
  private def tableDefinitions = {
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
        |INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', ${root.time});
        |""".stripMargin split ";"
  }

  // DataEntriesStopIdx: Find start of free data.
  // DataEntriesLengthHashIdx: Find data entries by size & hash.
  // TreeEntriesDataIdIdx: Find orphan data entries.
  private def indexDefinitions =
    """|CREATE INDEX DataEntriesStopIdx ON DataEntries(stop);
       |CREATE INDEX DataEntriesLengthHashIdx ON DataEntries(length, hash);
       |CREATE INDEX TreeEntriesDataIdIdx ON TreeEntries(dataId);""".stripMargin split ";"

  def initialize(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
    tableDefinitions.foreach(stat.executeUpdate)
    indexDefinitions.foreach(stat.executeUpdate)
  }

  def startOfFreeData(statement: Statement): Long =
    statement.executeQuery("SELECT MAX(stop) FROM DataEntries").pipe(_.maybeNext(_.getLong(1)).getOrElse(0L))

  def allDataChunks(statement: Statement): Seq[(Long, Long)] =
    statement.executeQuery("SELECT start, stop FROM DataEntries").seq(r => r.getLong(1) -> r.getLong(2))

  /** @return Seq(id, Option(size), Option(hash), seq, start, stop) */
  def allDataEntries(statement: Statement): Seq[(Long, Option[Long], Option[Array[Byte]], Int, Long, Long)] =
    statement.executeQuery("SELECT id, length, hash, seq, start, stop FROM DataEntries")
      .seq(r => (r.getLong(1), r.opt(_.getLong(2)), r.opt(_.getBytes(3)), r.getInt(4), r.getLong(5), r.getLong(6)))

  def stats(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
    log.info(s"Database statistics:")
    log.info(f"Folders: ${
      stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Files: ${
      stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Deleted folders: ${
      stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Deleted files: ${
      stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Orphan tree entries: ${
      stat.executeQuery("SELECT count(a.id) FROM TreeEntries a LEFT JOIN TreeEntries b ON a.parentId = b.id WHERE b.id IS NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Zero length data references: ${
      stat.executeQuery("SELECT COUNT(DISTINCT(t.dataid)) FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataId = d.id WHERE t.dataId IS NOT NULL AND d.id IS NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Referenced data entries: ${
      stat.executeQuery("SELECT COUNT(DISTINCT(t.dataid)) FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataId = d.id WHERE t.dataId IS NOT NULL AND t.deleted = 0 AND d.id IS NOT NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Referenced data entries including deleted files: ${
      stat.executeQuery("SELECT COUNT(DISTINCT(t.dataid)) FROM TreeEntries t LEFT JOIN DataEntries d ON t.dataId = d.id WHERE t.dataId IS NOT NULL AND d.id IS NOT NULL").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Total data entries: ${
      stat.executeQuery("SELECT COUNT(id) FROM DataEntries").tap(_.next()).getLong(1)
    }%,d")
    log.info(f"Size of orphan data entries: Not yet implemented")
    log.info(f"Referenced data storage (1): ${
      stat.executeQuery("SELECT SUM(stop-start) FROM DataEntries").tap(_.next()).getLong(1)/1000000000
    }%,d GB")
    log.info(f"Referenced data storage (2): ${
      stat.executeQuery("SELECT SUM(length) FROM DataEntries WHERE seq = 1").tap(_.next()).getLong(1)/1000000000
    }%,d GB")
    log.info(f"Total data storage: ${
      stat.executeQuery("SELECT MAX(stop) FROM DataEntries").tap(_.next()).getLong(1)/1000000000
    }%,d GB")
    stat.execute("CREATE CACHED TEMPORARY TABLE Referenced (id BIGINT PRIMARY KEY) AS SELECT DISTINCT(dataid) FROM TreeEntries WHERE dataid IS NOT NULL AND deleted = 0")
    val deletedEntries =
      stat.executeQuery("SELECT DISTINCT(t.dataId), d.length, * FROM TreeEntries t JOIN DataEntries d ON t.dataId = d.id LEFT JOIN Referenced r ON t.dataId = r.id WHERE t.dataId IS NOT NULL AND t.deleted != 0 AND d.seq = 1 AND r.id IS NULL")
        .seq(r => r.getLong(1) -> r.getLong(2))
    log.info(f"Size of deleted storage: ${deletedEntries.map(_._2).sum/1000000}%,d MB in ${deletedEntries.size} entries")
    stat.execute("DROP TABLE Referenced")
    // CREATE CACHED TEMPORARY TABLE Referenced (id BIGINT PRIMARY KEY) AS SELECT DISTINCT(dataid) FROM TreeEntries WHERE dataid IS NOT NULL AND deleted = 0;
    // SELECT DISTINCT(t.dataid), d.LENGTH, * FROM TreeEntries t JOIN DataEntries d ON t.dataid = d.id LEFT JOIN Referenced r ON t.dataid = r.id WHERE t.dataid IS NOT NULL AND t.deleted != 0 AND d.seq = 1 AND r.id IS NULL;
    // DROP TABLE Referenced;
//    val deleted = stat.executeQuery("SELECT DISTINCT(dataid) FROM TreeEntries WHERE deleted <> 0 AND dataid IS NOT NULL")
//      .seq(_.getLong(1)).toSet
//    val current = stat.executeQuery("SELECT DISTINCT(dataid) FROM TreeEntries WHERE deleted = 0 AND dataid IS NOT NULL")
//      .seq(_.getLong(1)).toSet
//    log.info(s"### ${(deleted--current).size} ${(deleted--current).take(100)}")
  }

  implicit class RichConnection(val c: Connection) extends AnyVal {
    /** Don't use nested or multithreaded. */
    def transaction[T](f: => T): T =
      try { c.setAutoCommit(false); f.tap(_ => c.commit()) }
      catch { case t: Throwable => c.rollback(); throw t }
      finally c.setAutoCommit(false)
  }

  implicit class RichResultSet(val rs: ResultSet) extends AnyVal {
    // TODO ResultSets should be handled as closeable resources
    def opt[T](f: ResultSet => T): Option[T] =
      f(rs).pipe(t => if (rs.wasNull) None else Some(t))
    def maybeNext[T](f: ResultSet => T): Option[T] =
      if (rs.next()) Some(f(rs)) else None
    def seq[T](f: ResultSet => T): Seq[T] =
      LazyList.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.to(List)
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
  {
    val dbVersionRead = resource(connection.createStatement())(
      _.executeQuery("SELECT value FROM Context WHERE key = 'db version'").pipe(_.maybeNext(_.getString(1)))
    )
    require(dbVersionRead.contains("2"), s"DB version read is $dbVersionRead, not 2")
  }

  private def sync[T](f: => T): T = synchronized(f)

  def startOfFreeData: Long = sync(resource(connection.createStatement())(Database.startOfFreeData))

  private val qChild = connection.prepareStatement(
    "SELECT id, time, dataId FROM TreeEntries WHERE parentId = ? AND name = ? AND deleted = 0"
  )
  def child(parentId: Long, name: String): Option[TreeEntry] = sync {
    qChild.setLong(1, parentId)
    qChild.setString(2, name)
    resource(qChild.executeQuery())(_.maybeNext(TreeEntry(parentId, name, _)))
  }

  private val qChildren = connection.prepareStatement(
    "SELECT id, time, dataId, name FROM TreeEntries WHERE parentId = ? AND deleted = 0"
  )
  def children(parentId: Long): Seq[TreeEntry] = sync {
    qChildren.setLong(1, parentId)
    resource(qChildren.executeQuery())(_.seq(rs => TreeEntry(parentId, rs.getString(4), rs)))
  }.filterNot(_.id == 0)

  private val qParts = connection.prepareStatement(
    "SELECT start, stop FROM DataEntries WHERE id = ? ORDER BY seq ASC"
  )
  def parts(dataId: Long): Parts = sync(Parts {
    qParts.setLong(1, dataId)
    resource(qParts.executeQuery())(_.seq { rs =>
      val (start, stop) = rs.getLong(1) -> rs.getLong(2)
      assumeLogged(start >= 0, s"start >= 0 ... $start")
      assumeLogged(stop >= start, s"stop >= start ... $stop / $start")
      start -> stop
    })
  })
  // TODO maybe read length from DB seq = 1 instead? (might return no entries)
  def dataSize(dataId: Long): Long = parts(dataId).size

  private val dTreeEntry = connection.prepareStatement(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  def delete(treeEntryId: Long): Boolean = sync {
    val time = System.currentTimeMillis.pipe { case 0 => 1; case x => x }
    dTreeEntry.setLong(1, time)
    dTreeEntry.setLong(2, treeEntryId)
    (dTreeEntry.executeUpdate() == 1).tap(r => if (!r) log.warn(s"DELETE tree entry $treeEntryId failed."))
  }

  private val uMoveRename = connection.prepareStatement(
    "UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?"
  )
  def moveRename(id: Long, newParentId: Long, newName: String): Boolean = sync {
    uMoveRename.setLong(1, newParentId)
    uMoveRename.setString(2, newName)
    uMoveRename.setLong(3, id)
    uMoveRename.executeUpdate() == 1
  }

  private val uTime = connection.prepareStatement(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  def setTime(id: Long, newTime: Long): Unit = sync {
    uTime.setLong(1, newTime)
    uTime.setLong(2, id)
    require(uTime.executeUpdate() == 1, s"setTime update count not 1 for id $id")
  }

  private val iDir = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  def mkDir(parentId: Long, name: String): Long = sync {
    iDir.setLong(1, parentId)
    iDir.setString(2, name)
    iDir.setLong(3, now)
    require(iDir.executeUpdate() == 1)
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }

  private val iFile = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, -1)",
    Statement.RETURN_GENERATED_KEYS
  )
  def mkFile(parentId: Long, name: String, time: Long): Long = sync {
    iFile.setLong(1, parentId)
    iFile.setString(2, name)
    iFile.setLong(3, time)
    require(iFile.executeUpdate() == 1)
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }

  private val iFileWithDataId = connection.prepareStatement(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)"
  )
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Unit = sync {
    iFileWithDataId.setLong(1, parentId)
    iFileWithDataId.setString(2, name)
    iFileWithDataId.setLong(3, time)
    iFileWithDataId.setLong(4, dataId)
    require(iFileWithDataId.executeUpdate() == 1)
  }

  private val qNextId = connection.prepareStatement("SELECT NEXT VALUE FOR idSeq")
  def nextId: Long = sync(resource(qNextId.executeQuery())(_.tap(_.next()).getLong(1)))

  // Generated keys seem not to be available for sql update, so this is two SQL commands
  def newDataIdFor(id: Long): Long = sync(nextId.tap(setDataId(id, _)))

  private val uDataId = connection.prepareStatement(
    "UPDATE TreeEntries SET dataId = ? WHERE id = ?"
  )
  def setDataId(id: Long, dataId: Long): Unit = sync {
    uDataId.setLong(1, dataId)
    uDataId.setLong(2, id)
    require(uDataId.executeUpdate() == 1, s"setDataId update count not 1 for id $id dataId $dataId")
  }

  private val qHash = connection.prepareStatement(
    "SELECT id FROM DataEntries WHERE hash = ? AND length = ?"
  )
  def dataEntry(hash: Array[Byte], size: Long): Option[Long] = sync {
    qHash.setBytes(1, hash)
    qHash.setLong(2, size)
    resource(qHash.executeQuery())(r => r.maybeNext(_.getLong(1)))
  }

  private val iDataEntry = connection.prepareStatement(
    "INSERT INTO DataEntries (id, seq, length, start, stop, hash) VALUES (?, ?, ?, ?, ?, ?)"
  )
  def insertDataEntry(dataId: Long, seq: Int, length: Long, start: Long, stop: Long, hash: Array[Byte]): Unit = sync {
    require(seq > 0, s"seq not positive: $seq")
    iDataEntry.setLong(1, dataId)
    iDataEntry.setInt(2, seq)
    if (seq == 1) iDataEntry.setLong(3, length) else iDataEntry.setNull(3, Types.BIGINT)
    iDataEntry.setLong(4, start)
    iDataEntry.setLong(5, stop)
    if (seq == 1) iDataEntry.setBytes(6, hash) else iDataEntry.setNull(3, Types.BINARY)
    require(iDataEntry.executeUpdate() == 1, s"insertDataEntry update count not 1 for dataId $dataId")
  }
}
