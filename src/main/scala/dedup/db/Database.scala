package dedup
package db

import dedup.db.Database.*

import java.io.File
import java.sql.{Connection, ResultSet, Statement, Types}
import scala.util.Try
import scala.util.Using.resource

def dbDir(repo: java.io.File) = java.io.File(repo, "fsdb")

def initialize(connection: Connection): Unit = connection.withStatement { stat =>
  tableDefinitions.foreach(stat.executeUpdate)
  indexDefinitions.foreach(stat.executeUpdate)
}

def withDb[T](dbDir: File, readonly: Boolean = true)(f: Database => T): T =
  withConnection(dbDir, readonly)(c => f(Database(c)))

object Database extends util.ClassLogging:
  val currentDbVersion = "3"

  // TODO move to Database class?
  def dbVersion(stat: Statement): Option[String] =
    stat.query("SELECT `VALUE` FROM Context WHERE `KEY` = 'db version'")(_.maybeNext(_.getString(1)))

  def checkAndMigrateDbVersion(stat: Statement): Unit =
    dbVersion(stat) match
      case None =>
        ensure("database.no.version", false, s"No database version found.")
      case Some(dbVersion) =>
        log.debug(s"Database version: $dbVersion.")
        ensure("database.illegal.version", dbVersion == currentDbVersion, s"Only database version $currentDbVersion is supported, detected version is $dbVersion.")

  def endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long]): (Long, Seq[DataArea]) =
    dataChunks.foldLeft(0L -> Vector.empty[DataArea]) {
      case ((lastEnd, gaps), (start, stop)) if start <= lastEnd =>
        ensure("data.find.gaps", start == lastEnd, s"Detected overlapping data entry ($start, $stop).")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(DataArea(lastEnd, start))
    }

  // TODO move to Database class?
  def endOfStorageAndDataGaps(statement: Statement): (Long, Seq[DataArea]) =
    val dataChunks = statement.query("SELECT start, stop FROM DataEntries")(_.seq(r => r.getLong(1) -> r.getLong(2)))
    val sortedChunks = dataChunks.to(scala.collection.SortedMap)
    log.debug(s"Number of data chunks in storage database: ${dataChunks.size}")
    ensure("data.sort.gaps", sortedChunks.size == dataChunks.size, s"${dataChunks.size - sortedChunks.size} duplicate chunk starts.")
    endOfStorageAndDataGaps(sortedChunks)

  def freeAreas(statement: Statement): Seq[DataArea] =
    val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(statement)
    log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
    log.info(s"Free for reclaiming: ${readableBytes(dataGaps.map(_.size).sum)} in ${dataGaps.size} gaps.")
    (dataGaps :+ DataArea(endOfStorage, Long.MaxValue)).tap(free => log.debug(s"Free areas: $free"))

class Database(connection: Connection) extends util.ClassLogging:
  val statement: Statement = connection.createStatement()
  import connection.{prepareStatement => prepare}
  checkAndMigrateDbVersion(statement)

  def freeAreas(): Seq[DataArea] = synchronized { Database.freeAreas(statement) }

  def split(path: String)       : Array[String] = path.split("/").filter(_.nonEmpty)
  def entry(path: String)       : Option[TreeEntry] = entry(split(path))
  def entry(path: Array[String]): Option[TreeEntry] = synchronized {
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }
  }

  /** ResultSet: (id, parentId, name, time, dataId) */
  private def treeEntry(rs: ResultSet): TreeEntry =
    TreeEntry(
      rs.getLong("id"),
      rs.getLong("parentId"),
      rs.getString("name"),
      Time(rs.getLong("time")),
      rs.opt(_.getLong("dataId")).map(DataId(_))
    )

  private val selectTreeEntry = "SELECT id, parentId, name, time, dataId FROM TreeEntries"

  private val qEntry = prepare(s"$selectTreeEntry WHERE id = ? AND deleted = 0")
  def entry(id: Long): Option[TreeEntry] = synchronized {
    qEntry.setLong(1, id)
    qEntry.query(_.maybeNext(treeEntry))
  }

  private val qEntryLike = prepare(s"$selectTreeEntry WHERE deleted = 0 AND name LIKE ?")
  def entryLike(nameLike: String): Seq[TreeEntry] = synchronized {
    qEntry.setString(1, nameLike)
    qEntry.query(_.seq(treeEntry))
  }

  private val qEntriesFor = prepare(s"$selectTreeEntry WHERE dataId = ? AND deleted = 0")
  def entriesFor(dataId: DataId): Seq[TreeEntry] = synchronized {
    qEntry.setLong(1, dataId.toLong)
    qEntry.query(_.seq(treeEntry))
  }

  def pathOf(id: Long): String = synchronized { pathOf(id, "") }
  @annotation.tailrec
  private def pathOf(id: Long, pathEnd: String): String =
    // Why not fold? - https://stackoverflow.com/questions/70821201/why-cant-option-fold-be-used-tail-recursively-in-scala
    // TODO eventually create a PR for scala-next enabling tailrec fold
    entry(id) match
      case None => s"[dangling]$pathEnd"
      case Some(entry: FileEntry) =>
        ensure("db.path.end", pathEnd == "", s"File $entry is not the path end, path end is $pathEnd.")
        pathOf(entry.parentId, entry.name) // Ignore path end if any in case ensure is suppressed.
      case Some(entry: DirEntry) =>
        val path = s"${entry.name}/$pathEnd"
        if entry.parentId == root.id then path else pathOf(entry.parentId, path)

  private val qChild = prepare(s"$selectTreeEntry WHERE parentId = ? AND name = ? AND deleted = 0")
  def child(parentId: Long, name: String): Option[TreeEntry] = synchronized {
    qChild.setLong  (1, parentId)
    qChild.setString(2, name    )
    qChild.query(_.maybeNext(treeEntry))
  }

  private val qChildren = prepare(s"$selectTreeEntry WHERE parentId = ? AND deleted = 0")
  def children(parentId: Long): Seq[TreeEntry] = synchronized {
    qChildren.setLong(1, parentId)
    qChildren.query(_.seq(treeEntry))
  }.filterNot(_.name.isEmpty) // On linux, empty names don't work, and the root node has itself as child...

  private val qParts = prepare(
    "SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC"
  )
  def parts(dataId: DataId): Vector[(Long, Long)] = synchronized {
    qParts.setLong(1, dataId.toLong)
    qParts.query(_.seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      ensure("data.part.start", start >= 0, s"Start $start must be >= 0.")
      ensure("data.part.size", size >= 0, s"Size $size must be >= 0.")
      start -> size
    })
  }.filterNot(_ == _) // Filter blacklisted parts of size 0.

  private val qDataSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size */
  def dataSize(dataId: DataId): Long = synchronized {
    qDataSize.tap(_.setLong(1, dataId.toLong)).query(_.maybeNext(_.getLong(1))).getOrElse(0)
  }
  /** @return the file's storage size */
  private val qStorageSize = prepare("SELECT stop - start FROM DataEntries WHERE id = ?")
  def storageSize(dataId: DataId): Long = synchronized {
    qStorageSize.tap(_.setLong(1, dataId.toLong)).query(_.seq(_.getLong(1))).sum
  }

  private val qDataEntry = prepare(
    "SELECT id FROM DataEntries WHERE hash = ? AND length = ?"
  )
  def dataEntry(hash: Array[Byte], size: Long): Option[DataId] = synchronized {
    qDataEntry.setBytes(1, hash)
    qDataEntry.setLong(2, size)
    qDataEntry.query(_.maybeNext(r => DataId(r.getLong(1))))
  }

  private val uTime = prepare(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  def setTime(id: Long, newTime: Long): Unit = synchronized {
    uTime.setLong(1, newTime)
    uTime.setLong(2, id)
    val count = uTime.executeUpdate()
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")
  }

  private val dTreeEntry = prepare(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  def delete(id: Long): Unit = synchronized {
    dTreeEntry.setLong(1, now.nonZero.toLong)
    dTreeEntry.setLong(2, id)
    val count = dTreeEntry.executeUpdate()
    ensure("db.delete", count == 1, s"For id $id, delete count is $count instead of 1.")
  }

  private val iDir = prepare(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS
  )
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = Try(synchronized {
    iDir.setLong  (1, parentId  )
    iDir.setString(2, name      )
    iDir.setLong  (3, now.toLong)
    val count = iDir.executeUpdate() // Name conflict triggers SQL exception due to unique constraint.
    ensure("db.mkdir", count == 1, s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }).toOption

  private val iFile = prepare(
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
    ensure("db.mkfile", count == 1, s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }).toOption

  private val uParentName = prepare(
    "UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?"
  )
  def update(id: Long, newParentId: Long, newName: String): Boolean = synchronized {
    uParentName.setLong  (1, newParentId)
    uParentName.setString(2, newName    )
    uParentName.setLong  (3, id         )
    uParentName.executeUpdate() == 1
  }

  private val uDataId = prepare(
    "UPDATE TreeEntries SET dataId = ? WHERE id = ?"
  )
  def setDataId(id: Long, dataId: DataId): Unit = synchronized {
    uDataId.setLong(1, dataId.toLong)
    uDataId.setLong(2, id)
    ensure("db.set.dataid", uDataId.executeUpdate() == 1, s"setDataId update count not 1 for id $id dataId $dataId")
  }

  private val qNextId = prepare(
    "SELECT NEXT VALUE FOR idSeq"
  )
  def nextId: Long = synchronized {
    qNextId.query(_.tap(_.next()).getLong(1))
  }

  def newDataIdFor(id: Long): DataId = synchronized {
    nextId.pipe(DataId(_)).tap(setDataId(id, _))
  }

  private val iDataEntry = prepare(
    "INSERT INTO DataEntries (id, seq, length, start, stop, hash) VALUES (?, ?, ?, ?, ?, ?)"
  )
  def insertDataEntry(dataId: DataId, seq: Int, length: Long, start: Long, stop: Long, hash: Array[Byte]): Unit = synchronized {
    ensure("db.add.data.entry.1", seq > 0, s"seq not positive: $seq")
    iDataEntry.setLong(1, dataId.toLong)
    iDataEntry.setInt(2, seq)
    if (seq == 1) iDataEntry.setLong(3, length) else iDataEntry.setNull(3, Types.BIGINT)
    iDataEntry.setLong(4, start)
    iDataEntry.setLong(5, stop)
    if (seq == 1) iDataEntry.setBytes(6, hash) else iDataEntry.setNull(3, Types.BINARY)
    ensure("db.add.data.entry.2", iDataEntry.executeUpdate() == 1, s"insertDataEntry update count not 1 for dataId $dataId")
  }

  def removeStorageAllocation(dataId: DataId): Unit = synchronized {
    connection.transaction { connection.withStatement { statement =>
      statement.executeUpdate(s"DELETE FROM DataEntries WHERE id = $dataId AND seq > 1")
      statement.executeUpdate(s"UPDATE DataEntries SET start = 0, stop = 0 WHERE id = $dataId")
    } }
  }

  def shutdownCompact(): Unit = synchronized {
    log.info("Compacting database...")
    statement.execute("SHUTDOWN COMPACT;")
  }

  def version(): Option[String] = synchronized { Database.dbVersion(statement) }

  // File system statistics
  def storageSize()      : Long = synchronized { statement.queryLongOrZero("SELECT MAX(stop) FROM DataEntries") }
  def countDataEntries() : Long = synchronized { statement.queryLongOrZero("SELECT COUNT(id) FROM DataEntries WHERE seq = 1") }
  def countFiles()       : Long = synchronized { statement.queryLongOrZero("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL") }
  def countDeletedFiles(): Long = synchronized { statement.queryLongOrZero("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL") }
  def countDirs()        : Long = synchronized { statement.queryLongOrZero("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL") }
  def countDeletedDirs() : Long = synchronized { statement.queryLongOrZero("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL") }

  // reclaimSpace specific queries
  /** @return The number of entries that have been un-rooted. */
  def unrootDeletedEntries(deleteBeforeMillis: Long): Long = synchronized {
    statement.executeLargeUpdate(s"UPDATE TreeEntries SET parentId = id WHERE deleted != 0 AND deleted < $deleteBeforeMillis")
  }
  /** @return The number of entries that have been deleted. */
  def deleteUnrootedTreeEntries(): Long = synchronized {
    statement.executeLargeUpdate(s"DELETE FROM TreeEntries WHERE id = parentId AND id != ${root.id}")
  }
  def dataIdsInTree(): Set[Long] = synchronized { statement.query(
    // The WHERE clause makes sure the 'null' entries are not returned
    "SELECT DISTINCT(dataId) FROM TreeEntries WHERE dataId >= 0"
  )(_.seq(_.getLong(1))).toSet }
  def dataIdsInStorage(): Set[Long] = synchronized { statement.query(
    // The WHERE clause makes sure the 'null' entries are not returned
    "SELECT id FROM DataEntries"
  )(_.seq(_.getLong(1))).toSet }
  def deleteDataEntry(dataId: Long): Unit = synchronized {
    val count = statement.executeUpdate(s"DELETE FROM DataEntries WHERE id = $dataId")
    ensure("db.delete.dataentry", count == 1, s"For data id $dataId, delete count is $count instead of 1.")
  }

extension (rawSql: String)
  private def prepareSql = rawSql.stripMargin.split(";")

private def tableDefinitions =
  s"""|CREATE TABLE Context (
      |  -- Starting with H2 2.0.202, KEY and VALUE are reserved keywords and must be quoted. --
      |  `KEY`   VARCHAR(255) NOT NULL,
      |  `VALUE` VARCHAR(255) NOT NULL,
      |  CONSTRAINT pk_Context PRIMARY KEY (`KEY`)
      |);
      |INSERT INTO Context (`KEY`, `VALUE`) VALUES ('db version', '$currentDbVersion');
      |CREATE SEQUENCE idSeq START WITH 1;
      |CREATE TABLE DataEntries (
      |  id     BIGINT NOT NULL,
      |  seq    INTEGER NOT NULL,
      |  length BIGINT NULL,
      |  start  BIGINT NOT NULL,
      |  stop   BIGINT NOT NULL,
      |  hash   BINARY(16) NULL,
      |  CONSTRAINT pk_DataEntries PRIMARY KEY (id, seq)
      |);
      |CREATE TABLE TreeEntries (
      |  id           BIGINT NOT NULL DEFAULT (NEXT VALUE FOR idSeq),
      |  parentId     BIGINT NOT NULL,
      |  name         VARCHAR(255) NOT NULL,
      |  time         BIGINT NOT NULL,
      |  -- deleted == 0 for regular files, deleted == timestamp for deleted files, because NULL does not work with UNIQUE --
      |  deleted      BIGINT NOT NULL DEFAULT 0,
      |  dataId       BIGINT DEFAULT NULL,
      |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
      |  CONSTRAINT un_TreeEntries UNIQUE (parentId, name, deleted),
      |  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
      |);
      |INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', $now);
      |""".prepareSql

private def indexDefinitions =
  """|-- Stats: Read active storage size --
     |CREATE INDEX DataEntriesStopIdx ON DataEntries(stop);
     |-- Find data entries by size & hash --
     |CREATE INDEX DataEntriesLengthHashIdx ON DataEntries(length, hash);
     |-- Find distinct data references, orphan data entries and blacklisted copies --
     |CREATE INDEX TreeEntriesDataIdIdx ON TreeEntries(dataId);""".prepareSql
