package dedup
package db

import dedup.db.Database.currentDbVersion

import java.io.File
import java.sql.{Connection, PreparedStatement, Statement}
import scala.util.{Failure, Success, Try}

def dbDir(repo: File) = File(repo, "fsdb")

def initialize(connection: Connection): Unit = connection.withStatement { stat =>
  tableDefinitions.foreach(stat.executeUpdate)
  indexDefinitions.foreach(stat.executeUpdate)
}

def withDb[T](dbDir: File, readOnly: Boolean, checkVersion: Boolean = true)(f: Database => T): T =
  withConnection(dbDir, readOnly)(connection => f(Database(connection, checkVersion)))

object Database extends util.ClassLogging:
  val currentDbVersion = "3"

  def endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long]): (Long, Seq[DataArea]) =
    dataChunks.foldLeft(0L -> Vector.empty[DataArea]) {
      case ((lastEnd, gaps), (start, stop)) if start <= lastEnd =>
        ensure("data.find.gaps", start == lastEnd, s"Detected overlapping data entry: End = $lastEnd, start = $start.")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(DataArea(lastEnd, start))
    }

  /** Characters not allowed in Windows file names. */
  private val illegalCharsInNames = Set('\\', '/', ':', '*', '?', '"', '<', '>', '|')

  def validateName(fileName: String): Unit =
    require(fileName.nonEmpty, "The name must not be empty.")
    val illegalChars = fileName.toSet.intersect(Database.illegalCharsInNames)
    if illegalChars.nonEmpty then
      val problemList = illegalChars.mkString("'", "','", "'")
      throw new IllegalArgumentException(s"The name '$fileName' contains illegal characters: $problemList")

/** Database queries and related things.
  *
  * Database resources are allocated on demand, so all database code can be collected in this class
  * without potential resource penalties even if it is not needed by all tools.
  * For this, `lazy val` is used for [[prepare]] instances. */
final class Database(connection: Connection, checkVersion: Boolean = true) extends AutoCloseable with util.ClassLogging:

  /** Close the database connection. */
  override def close(): Unit = connection.close()

  private def withStatement[T](f: Statement => T): T = connection.withStatement(f)

  /** Utility class making sure a [[PreparedStatement]] is used synchronized
    * because in many cases [[PreparedStatement]]s are stateful.
    *
    * @param sql             The SQL string to prepare as [[PreparedStatement]].
    * @param returnGenerated If [[true]] then [[Statement.RETURN_GENERATED_KEYS]] will be set. */
  private class prepare(sql: String, returnGenerated: Boolean = false):
    private val prep =
      if returnGenerated then connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      else connection.prepareStatement(sql)
    def apply[T](f: PreparedStatement => T): T = synchronized(f(prep))

  // Check and (possibly) migrate database version
  if checkVersion then version() match
    case None =>
      problem("database.no.version", s"No database version found.")
    case Some(dbVersion) =>
      log.debug(s"Database version: $dbVersion.")
      ensure("database.illegal.version", dbVersion == currentDbVersion, s"Only database version $currentDbVersion is supported, detected version is $dbVersion.")

  /** @return The version string read from the database if any. */
  def version(): Option[String] = Try(
    withStatement(_.query("SELECT `VALUE` FROM Context WHERE `KEY` = 'db version'")(maybe(_.getString(1))))
  ).toOption.flatten

  /* The starts and stops of the contiguous data areas can be read like this:
     SELECT b1.start FROM DataEntries b1 LEFT JOIN DataEntries b2
         ON b1.start = b2.stop WHERE b2.stop IS NULL ORDER BY b1.start;
     However, it's faster to read all DataEntries and sort them in Scala like below, assuming there's enough memory. */
  def freeAreas(): Seq[DataArea] =
    val dataChunks = withStatement(
      _.query("SELECT start, stop FROM DataEntries")(seq(r => r.getLong(1) -> r.getLong(2))).filterNot(_ == (0, 0))
    )
    log.debug(s"Number of data chunks in storage database: ${dataChunks.size}")
    val sortedChunks = dataChunks.to(scala.collection.SortedMap)
    val (endOfStorage, dataGaps) = Database.endOfStorageAndDataGaps(sortedChunks)
    log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
    log.info(s"Free for reclaiming: ${readableBytes(dataGaps.map(_.size).sum)} in ${dataGaps.size} gaps.")
    if sortedChunks.size != dataChunks.size then
      log.error(s"${dataChunks.size - sortedChunks.size} duplicate chunk starts.")
      val problems = dataChunks.groupBy(_._1)
        .collect { case (_, entries) if entries.length > 1 => entries }.flatten.toSeq
      if problems.length < 200 then
        log.error(s"Duplicates: $problems")
      else
        log.error(s"First 200 duplicates: ${problems.take(200)}")
      problem("data.sort.gaps", s"Database might be corrupt. Restore from backup?")
    (dataGaps :+ DataArea(endOfStorage, Long.MaxValue)).tap(logFreeAreas)

  private def logFreeAreas(free: Seq[DataArea]): Unit =
    if free.size < 100 then log.debug(s"Free areas: $free")
    else log.debug(s"First 100 of ${free.size} free areas: ${free.take(100)}")
  
  /** @return The path elements of this file system path. */
  def pathElements(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: String): Option[TreeEntry] = entry(pathElements(path))
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }

  /** From a [[selectTreeEntry]] query extract the [[TreeEntry]] data. */
  private def treeEntry(rs: java.sql.ResultSet): TreeEntry =
    TreeEntry(
      rs.getLong("id"),
      rs.getLong("parentId"),
      rs.getString("name"),
      Time(rs.getLong("time")),
      rs.opt(_.getLong("dataId")).map(DataId(_))
    )
  /** Select [[TreeEntry]] data that can be extracted using the [[treeEntry]] method. */
  private val selectTreeEntry = "SELECT id, parentId, name, time, dataId FROM TreeEntries"

  private lazy val qEntry = prepare(s"$selectTreeEntry WHERE id = ? AND deleted = 0")
  /** @return The [[TreeEntry]] if any. */
  def entry(id: Long): Option[TreeEntry] = qEntry(_.set(id).query(maybe(treeEntry)))

  private lazy val qEntryLike = prepare(s"$selectTreeEntry WHERE deleted = 0 AND name LIKE ?")
  /** @return All [[TreeEntry]] instances having a name `SQL:like` the requested. */
  def entryLike(nameLike: String): Seq[TreeEntry] = qEntryLike(_.set(nameLike).query(seq(treeEntry)))

  private lazy val qEntriesFor = prepare(s"$selectTreeEntry WHERE dataId = ? AND deleted = 0")
  /** @return All [[TreeEntry]] instances referring to the `dataId`. */
  def entriesFor(dataId: DataId): Seq[TreeEntry] = qEntriesFor(_.set(dataId).query(seq(treeEntry)))

  /** @return The path of the tree entry. If a path element is not found, marks it as `[not found]/`. */
  def pathOf(id: Long): String = pathOf(id, "")
  @annotation.tailrec
  private def pathOf(id: Long, pathEnd: String): String =
    // Why not fold? - https://stackoverflow.com/questions/70821201/why-cant-option-fold-be-used-tail-recursively-in-scala
    if id == root.id then s"/$pathEnd"
    else entry(id) match
      case None => s"[not found]/$pathEnd"
      case Some(entry: FileEntry) =>
        ensure("db.path.end", pathEnd == "", s"File $entry is not the path end, path end is $pathEnd.")
        if pathEnd == "" then pathOf(entry.parentId, entry.name)
        else pathOf(entry.parentId, s"${entry.name}/$pathEnd")
      case Some(entry: DirEntry) => pathOf(entry.parentId, s"${entry.name}/$pathEnd")

  private lazy val qChild = prepare(s"$selectTreeEntry WHERE parentId = ? AND name = ? AND deleted = 0")
  def child(parentId: Long, name: String): Option[TreeEntry] =
    qChild(_.set(parentId, name).query(maybe(treeEntry)))

  private lazy val qChildren = prepare(s"$selectTreeEntry WHERE parentId = ? AND deleted = 0")
  def children(parentId: Long): Seq[TreeEntry] =
    // On linux, empty names don't work, and the root node has itself as child...
    qChildren(_.set(parentId).query(seq(treeEntry))).filterNot(_.name.isEmpty)

  private lazy val qParts = prepare("SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC")
  def parts(dataId: DataId): Seq[(Long, Long)] =
    qParts(_.set(dataId).query(seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      ensure("data.part.start", start >= 0, s"Start $start must be >= 0.")
      ensure("data.part.size", size >= 0, s"Size $size must be >= 0.")
      start -> size
    })).filterNot(_._2 == 0) // Filter parts of size 0 as created when blacklisting.

  private lazy val qLogicalSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size for the data entry or 0 if there is no matching data entry. */
  def logicalSize(dataId: DataId): Long = qLogicalSize(_.set(dataId).query(maybe(_.getLong(1))).getOrElse(0))

  private val qStorageSize = prepare("SELECT stop - start FROM DataEntries WHERE id = ?")
  /** @return The file's storage size. */
  def storageSize(dataId: DataId): Long = qStorageSize(_.set(dataId).query(seq(_.getLong(1)))).sum

  private val qDataEntry = prepare("SELECT id FROM DataEntries WHERE hash = ? AND length = ?")
  /** @return The matching [[DataId]] if any. */
  def dataEntry(hash: Array[Byte], size: Long): Option[DataId] = qDataEntry(_.set(hash, size).query(maybe(r => DataId(r.getLong(1)))))

  private lazy val uTime = prepare("UPDATE TreeEntries SET time = ? WHERE id = ?")
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs, but may be
    * called for deleted entries. */
  def setTime(id: Long, newTime: Time): Unit =
    val count = uTime(_.set(newTime, id).executeUpdate())
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")

  private lazy val dTreeEntry = prepare("UPDATE TreeEntries SET deleted = ? WHERE id = ?")
  /** Marks a childless tree entry deleted.
    *
    * When used multi-threaded together with other tree structure methods, needs external synchronization to prevent
    * race conditions causing deleted directories to be parent of non-deleted tree entries.
    *
    * @return `false` if the tree entry does not exist or has any children, `true` if the entry exists and has no
    *         children, regardless of whether or not it was already marked deleted. */
  def deleteChildless(id: Long): Boolean = dTreeEntry { prep =>
    // Allow to delete 'illegal' nodes that have themselves as parent.
    if children(id).forall(_.id == id) then prep.set(now.nonZero, id).executeUpdate() > 0 else false
  }

  private lazy val iDir = prepare("INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", true)
  /** Creates a directory. It is the responsibility of the calling code to guard against creating a directory as child
    * of a file or as child of a deleted directory.
    *
    * When used multi-threaded together with other tree structure methods, needs external synchronization to prevent
    * race conditions causing deleted directories to be parent of non-deleted tree entries.
    *
    * @return [[Some]]`(fileId)` or [[None]] in case of a name conflict.
    * @throws Exception If the parent does not exist or the name is empty. */
  def mkDir(parentId: Long, name: String): Option[Long] =
    Database.validateName(name)
    Try(iDir { prep =>
      // Name conflict or missing parent triggers an SQL exception due to unique constraint / foreign key violation.
      prep.set(parentId, name, now).executeUpdate()
      guardedGetGeneratedId(parentId, name, prep)
    }).pipe(handleMkResult(parentId, name, "mkDir"))

  private lazy val iFile = prepare("INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)", true)
  /** Creates a file. It is the responsibility of the calling code to guard against creating a file as child
    * of a file or as child of a deleted directory.
    *
    * When used multi-threaded together with other tree structure methods, needs external synchronization to prevent
    * race conditions causing deleted directories to be parent of non-deleted tree entries.
    *
    * @return [[Some]]`(fileId)` or [[None]] in case of a name conflict.
    * @throws Exception If the parent does not exist or the name is empty. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Option[Long] =
    Database.validateName(name)
    Try(iFile { prep =>
      // Name conflict or missing parent triggers an SQL exception due to unique constraint / foreign key violation.
      prep.set(parentId, name, time, dataId).executeUpdate()
      guardedGetGeneratedId(parentId, name, prep)
    }).pipe(handleMkResult(parentId, name, "mkFile"))

  private def guardedGetGeneratedId(parentId: Long, name: String, prep: PreparedStatement): Long =
    prep.getGeneratedKeys.tap(_.next()).getLong("id").tap { id =>
      if parentId == id then // Special case of "parent does not exist" where the created node has a parent self reference.
        deleteChildless(id)
        failure(s"For parentId $parentId and name '$name', the id of the created directory was the same as the parentId.")
    }

  private def handleMkResult(parentId: Long, name: String, methodName: String): Try[Long] => Option[Long] = {
    case Success(id) => Some(id)
    case Failure(e: java.sql.SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
      log.trace(s"$methodName($parentId, '$name'): Name conflict."); None
    case Failure(other) => throw other
  }

  private lazy val uRenameMove = prepare("UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?")
  /** Updates the parent/name of a tree entry. It is the responsibility of the calling code to guard against creating a
    * a tree entry as child of a file or as child of a deleted directory.
    *
    * When used multi-threaded together with other tree structure methods, needs external synchronization to prevent
    * race conditions causing deleted directories to be parent of non-deleted tree entries.
    *
    * @return `true` on success, `false` in case of a name conflict or if the tree entry does not exist.
    * @throws Exception If the new parent does not exist or the new name is empty. */
  def renameMove(id: Long, newParentId: Long, newName: String): Boolean =
    require(newName.nonEmpty, "The name must not be empty.")
    Try(uRenameMove { prep =>
      // Name conflict or missing parent triggers an SQL exception due to unique constraint / foreign key violation.
      prep.set(newParentId, newName, id).executeUpdate() > 0
    }) match
      case Success(value) => value
      case Failure(e: java.sql.SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"renameMove($id, $newParentId, '$newName'): Name conflict."); false
      case Failure(other) => throw other

  private lazy val uDataId = prepare("UPDATE TreeEntries SET dataId = ? WHERE id = ?")
  def setDataId(id: Long, dataId: DataId): Unit = synchronized {
    val count = uDataId(_.set(dataId, id).executeUpdate())
    ensure("db.set.dataid", count == 1, s"setDataId update count is $count and not 1 for id $id dataId $dataId")
  }

  private lazy val qNextId = prepare("SELECT NEXT VALUE FOR idSeq")
  def newDataId(): DataId = DataId(qNextId(_.query(next(_.getLong(1)))))

  private lazy val iDataEntry = prepare("INSERT INTO DataEntries (id, seq, length, start, stop, hash) VALUES (?, ?, ?, ?, ?, ?)")
  def insertDataEntry(dataId: DataId, seq: Int, length: Long, start: Long, stop: Long, hash: Array[Byte]): Unit =
    ensure("db.add.dataEntry.1", seq > 0, s"seq not positive: $seq")
    val sqlLength: SqlNull | Long        = if seq == 1 then length else SqlNull(java.sql.Types.BIGINT)
    val sqlHash  : SqlNull | Array[Byte] = if seq == 1 then hash   else SqlNull(java.sql.Types.BINARY)
    val count = iDataEntry(_.set(dataId, seq, sqlLength, start, stop, sqlHash).executeUpdate())
    ensure("db.add.dataEntry.2", count == 1, s"insertDataEntry update count is $count and not 1 for dataId $dataId")

  /** Transaction implementation is not thread safe, use in single threaded environment only. */
  def removeStorageAllocation(dataId: DataId): Unit =
    connection.transaction {
      withStatement { statement =>
        statement.executeUpdate(s"DELETE FROM DataEntries WHERE id = $dataId AND seq > 1")
        statement.executeUpdate(s"UPDATE DataEntries SET start = 0, stop = 0 WHERE id = $dataId")
      }
    }

  def shutdownCompact(): Unit = H2.shutdownCompact(connection)

  // File system statistics
  def storageSize()      : Long = withStatement(_.query("SELECT MAX(stop) FROM DataEntries")(one(_.opt(_.getLong(1)))).getOrElse(0L))
  def countDataEntries() : Long = withStatement(_.query("SELECT COUNT(id) FROM DataEntries WHERE seq = 1")(oneLong))
  def countFiles()       : Long = withStatement(_.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL")(oneLong))
  def countDeletedFiles(): Long = withStatement(_.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL")(oneLong))
  def countDirs()        : Long = withStatement(_.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL")(oneLong))
  def countDeletedDirs() : Long = withStatement(_.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL")(oneLong))

  // reclaimSpace specific queries
  /** @return The number of tree entries that have been un-rooted. */
  def unrootDeletedEntries(deleteBeforeMillis: Long): Long =
    withStatement(_.executeLargeUpdate(s"UPDATE TreeEntries SET parentId = id WHERE deleted != 0 AND deleted < $deleteBeforeMillis"))

  /** @return The IDs of tree entries having un-rooted deleted parents. */
  def childrenOfDeletedUnrootedElements(): Seq[Long] =
    withStatement(_.query(
      "SELECT Child.id FROM TreeEntries Parent LEFT JOIN TreeEntries Child ON Parent.id = Child.parentId " +
      "WHERE Parent.id != Child.id AND Parent.id = Parent.ParentId AND Parent.deleted != 0"
    )(seq(_.getLong(1))))

  /** The ID of the tree entry to unroot should exist. */
  def unrootAndMarkDeleted(id: Long): Unit =
    val count = withStatement(_.executeUpdate(s"UPDATE TreeEntries SET parentId = $id, deleted = 1 WHERE id = $id"))
    ensure("db.unrootEntry", count == 1, s"unrootEntry update count is $count and not 1 for id $id")

  /** @return The number of tree entries that have been deleted. */
  def deleteUnrootedEntries(): Long =
    withStatement(_.executeLargeUpdate(s"DELETE FROM TreeEntries WHERE id = parentId AND id != ${root.id}"))

  def dataIdsInTree(): Set[Long] =
    // The 'WHERE dataId >= 0' clause filters both 'dataId < 0' and 'dataId IS NULL' entries
    withStatement(_.query("SELECT DISTINCT(dataId) FROM TreeEntries WHERE dataId >= 0")(seq(_.getLong(1))).toSet)

  def dataIdsInStorage(): Set[Long] =
    withStatement(_.query("SELECT id FROM DataEntries")(seq(_.getLong(1))).toSet)

  def deleteDataEntry(dataId: Long): Unit =
    val count = withStatement(_.executeUpdate(s"DELETE FROM DataEntries WHERE id = $dataId"))
    ensure("db.delete.dataEntry", count == 1, s"For data id $dataId, delete count is $count instead of 1.")


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
      |  -- length is NULL for all entries with seq > 1
      |  length BIGINT NULL,
      |  start  BIGINT NOT NULL,
      |  stop   BIGINT NOT NULL,
      |  -- hash is NULL for all entries with seq > 1
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
      |  -- No foreign key relation possible because DataEntries(id) is not unique.
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
