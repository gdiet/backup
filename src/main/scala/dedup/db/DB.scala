package dedup
package db

import dedup.db.Database.currentDbVersion

import java.sql.{PreparedStatement, Statement}
import scala.util.{Failure, Success, Try}
import scala.util.Using.resource

/** Database queries and related things.
  *
  * Database resources are allocated on demand, so all database code can be collected in this class
  * without potential resource penalties even if it is not needed by all tools.
  * For this, `lazy val` is used for [[prepare]] instances. */
final class DB(connection: java.sql.Connection, checkVersion: Boolean = true) extends AutoCloseable with util.ClassLogging:

  /** Close the database connection. */
  override def close(): Unit = connection.close()

  private def statement[T](f: Statement => T): T = resource(connection.createStatement)(f)

  /** Utility class making sure a [[PreparedStatement]] is used synchronized
    * because in many cases [[PreparedStatement]]s are stateful.
    *
    * @param sql             The SQL string to prepare as [[PreparedStatement]].
    * @param returnGenerated If [[true]] then [[Statement.RETURN_GENERATED_KEYS]] will be set.
    * @param monitor         The monitor to use for synchronization.
    *                        If [[None]] (default), synchronizing on this [[prepare]] instance. */
  private class prepare(sql: String, returnGenerated: Boolean = false, monitor: Option[Object] = None):
    private val prep =
      if returnGenerated then connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      else connection.prepareStatement(sql)
    private val sync = monitor.getOrElse(this)
    def apply[T](f: PreparedStatement => T): T = sync.synchronized(f(prep))

  /** Synchronization monitor used to prevent race conditions where tree entries with children could be deleted. */
  private object TreeStructureMonitor
  /** To prevent race conditions where a directory is deleted although it contains children, all statements modifying
    * the tree structure share a common synchronization monitor. Remember to use this method where needed. */
  private def prepareTreeModification(sql: String, returnGenerated: Boolean = false) =
    prepare(sql, returnGenerated, Some(TreeStructureMonitor))

  // Check and (possibly) migrate database version
  if checkVersion then version() match
    case None =>
      problem("database.no.version", s"No database version found.")
    case Some(dbVersion) =>
      log.debug(s"Database version: $dbVersion.")
      ensure("database.illegal.version", dbVersion == currentDbVersion, s"Only database version $currentDbVersion is supported, detected version is $dbVersion.")

  /** @return The version string read from the database if any. */
  def version(): Option[String] = Try(
    statement(_.query("SELECT `VALUE` FROM Context WHERE `KEY` = 'db version'")(maybe(_.getString(1))))
  ).toOption.flatten

  /* The starts and stops of the contiguous data areas can be read like this:
     SELECT b1.start FROM DataEntries b1 LEFT JOIN DataEntries b2
         ON b1.start = b2.stop WHERE b2.stop IS NULL ORDER BY b1.start;
     However, it's faster to read all DataEntries and sort them in Scala like below, assuming there's enough memory. */
  def freeAreas(): Seq[DataArea] =
    val dataChunks = statement(
      _.query("SELECT start, stop FROM DataEntries")(seq(r => r.getLong(1) -> r.getLong(2))).filterNot(_ == (0, 0))
    )
    log.debug(s"Number of data chunks in storage database: ${dataChunks.size}")
    val sortedChunks = dataChunks.to(scala.collection.SortedMap)
    val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(sortedChunks)
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
    (dataGaps :+ DataArea(endOfStorage, Long.MaxValue)).tap(free => log.debug(s"Free areas: $free"))

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

  // TODO check whether it would be better to return Area instead of position+size
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
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit =
    val count = uTime(_.set(newTime, id).executeUpdate())
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")

  private lazy val dTreeEntry = prepareTreeModification("UPDATE TreeEntries SET deleted = ? WHERE id = ?")
  /** Deletes a tree entry. Should be called only for existing entry IDs.
    * @return `false` if the tree entry has children. */
  def deleteChildless(id: Long): Boolean = dTreeEntry { prep =>
    // Allow to delete 'illegal' nodes that have themselves as parent.
    if children(id).filterNot(_.id == id).nonEmpty then false else
      val count = prep.set(now.nonZero, id).executeUpdate()
      ensure("db.delete", count == 1, s"For id $id, delete count is $count instead of 1.")
      count > 0
  }

  private lazy val iDir = prepareTreeModification("INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", true)
  /** @return `Some(id)` or [[None]] if a child with the same name exists.
    * @throws Exception If parent does not exist. */
  def mkDir(parentId: Long, name: String): Option[Long] =
    require(name.nonEmpty, "Can not create a directory with an empty name.")
    Try(iDir { prep =>
      // Name conflict or missing parent triggers SQL exception due to unique constraint / foreign key.
      val count = prep.set(parentId, name, now).executeUpdate()
      ensure("db.mkdir", count == 1, s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
      prep.getGeneratedKeys.tap(_.next()).getLong("id").tap { id =>
        if parentId == id then
          deleteChildless(id)
          problem("db.mkdir.sameAsParent", s"For parentId $parentId and name '$name', the id of the created directory was the same as the parentId.")
      }
    }) match
      case Success(id) => Some(id)
      case Failure(e: java.sql.SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"mkDir($parentId, '$name'): Name conflict."); None
      case Failure(other) => throw other

  private lazy val iFile = prepareTreeModification("INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)", true)
  /** @return `Some(id)` or [[None]] if a child with the same name exists.
    * @throws Exception If parent does not exist. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Option[Long] =
    require(name.nonEmpty, "Can not create a file with an empty name.")
    Try(iFile { prep =>
      // Name conflict or missing parent triggers an SQL exception due to unique constraint / foreign key violation.
      val count = prep.set(parentId, name, time, dataId).executeUpdate()
      ensure("db.mkfile", count == 1, s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
      prep.getGeneratedKeys.tap(_.next()).getLong("id").tap { id =>
        if parentId == id then
          deleteChildless(id)
          problem("db.mkFile.sameAsParent", s"For parentId $parentId and name '$name', the id of the created file was the same as the parentId.")
      }
    }) match
      case Success(id) => Some(id)
      case Failure(e: java.sql.SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"mkFile($parentId, '$name', $time, $dataId): Name conflict."); None
      case Failure(other) => throw other

  private lazy val uRenameMove = prepareTreeModification("UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?")
  /** @return `true` on success, `false` in case of a name conflict.
    * @throws Exception If new parent does not exist or new name is empty. */
  def renameMove(id: Long, newParentId: Long, newName: String): Boolean =
    require(newName.nonEmpty, "Can't rename to an empty name.")
    Try(uRenameMove { prep =>
      // Name conflict or missing parent triggers SQL exception due to unique constraint / foreign key.
      val count = prep.set(newParentId, newName, id).executeUpdate()
      ensure("db.renameMove", count == 1, s"For id $id, renameMove count is $count instead of 1.")
      count > 0 // TODO consider expected and actual behavior if tree node does not exist, is deleted, or parent is deleted.
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
    ensure("db.add.data.entry.1", seq > 0, s"seq not positive: $seq")
    val sqlLength: Long | SqlNull = if seq == 1 then length else SqlNull(java.sql.Types.BIGINT)
    val sqlHash: Array[Byte] | SqlNull = if seq == 1 then hash else SqlNull(java.sql.Types.BINARY)
    val count = iDataEntry(_.set(dataId, seq, sqlLength, start, stop, sqlHash).executeUpdate())
    ensure("db.add.data.entry.2", count == 1, s"insertDataEntry update count is $count and not 1 for dataId $dataId")

  private def endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long]): (Long, Seq[DataArea]) =
    dataChunks.foldLeft(0L -> Vector.empty[DataArea]) {
      case ((lastEnd, gaps), (start, stop)) if start <= lastEnd =>
        ensure("data.find.gaps", start == lastEnd, s"Detected overlapping data entry: End = $lastEnd, start = $start.")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(DataArea(lastEnd, start))
    }
