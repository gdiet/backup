package dedup
package db

import java.sql.{Connection, SQLException, Statement, Types}
import scala.util.{Failure, Success, Try}
import scala.util.Using.resource

final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with util.ClassLogging:
  import connection.prepareStatement as prepare

  private def statement[T](f: Statement => T): T = resource(connection.createStatement())(f)

  def shutdownCompact(): Unit =
    log.info("Compacting database...")
    statement(_.execute("SHUTDOWN COMPACT"))

  // Start: Pure read methods that are only used in the write backend
  private val qDataEntry = prepare("SELECT id FROM DataEntries WHERE hash = ? AND length = ?")
  def dataEntry(hash: Array[Byte], size: Long): Option[DataId] =
    qDataEntry.sync(_.set(hash, size).query(maybe(r => DataId(r.getLong(1)))))

  /* The starts and stops of the contiguous data areas can be read like this:
     SELECT b1.start FROM DataEntries b1 LEFT JOIN DataEntries b2
         ON b1.start = b2.stop WHERE b2.stop IS NULL ORDER BY b1.start;
     However, it's faster to read all DataEntries and sort them in Scala like below, assuming there's enough memory. */
  def freeAreas(): Seq[DataArea] =
    val dataChunks = statement(
      _.query("SELECT start, stop FROM DataEntries")(seq(r => r.getLong(1) -> r.getLong(2)))
      .filterNot(_ == (0, 0)))
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

  private def endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long]): (Long, Seq[DataArea]) =
    dataChunks.foldLeft(0L -> Vector.empty[DataArea]) {
      case ((lastEnd, gaps), (start, stop)) if start <= lastEnd =>
        ensure("data.find.gaps", start == lastEnd, s"Detected overlapping data entry: End = $lastEnd, start = $start.")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(DataArea(lastEnd, start))
    }
  // End: Pure read methods that are only used in the write backend

  /** Synchronization monitor used to avoid that tree entries with children are deleted. */
  private object TreeStructureMonitor
  private def structureSync[T](f: => T): T = TreeStructureMonitor.synchronized(f)

  private val iDir = prepare(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  /** @return `Some(id)` or [[None]] if a child with the same name exists.
    * @throws Exception If parent does not exist. */
  def mkDir(parentId: Long, name: String): Option[Long] =
    require(name.nonEmpty, "Can't create a dir with an empty name.")
    Try(structureSync {
      // Name conflict or missing parent triggers SQL exception due to unique constraint / foreign key.
      val count = iDir.set(parentId, name, now).executeUpdate()
      ensure("db.mkdir", count == 1, s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
      iDir.getGeneratedKeys.tap(_.next()).getLong("id").tap { id =>
        if parentId == id then
          deleteChildless(id)
          problem("db.mkdir.2", s"For parentId $parentId $id and name '$name', the id of the created dir was the same as the parentId.")
      }
    }) match
      case Success(id) => Some(id)
      case Failure(e: SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"mkDir($parentId, '$name'): Name conflict."); None
      case Failure(other) => throw other

  private val iFile = prepare(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  /** @return `Some(id)` or [[None]] if a child with the same name exists.
    * @throws Exception If parent does not exist. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Option[Long] =
    require(name.nonEmpty, "Can't create a file with an empty name.")
    Try(structureSync {
      // Name conflict or missing parent triggers SQL exception due to unique constraint / foreign key.
      val count = iFile.set(parentId, name, time, dataId).executeUpdate()
      ensure("db.mkfile", count == 1, s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
      iFile.getGeneratedKeys.tap(_.next()).getLong("id").tap { id =>
        if parentId == id then
          deleteChildless(id)
          problem("db.mkFile.2", s"For parentId $parentId $id and name '$name', the id of the created file was the same as the parentId.")
      }
    }) match
      case Success(id) => Some(id)
      case Failure(e: SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"mkFile($parentId, '$name', $time, $dataId): Name conflict."); None
      case Failure(other) => throw other

  private val uTime = prepare("UPDATE TreeEntries SET time = ? WHERE id = ?")
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit =
    val count = uTime.sync(_.set(newTime, id).executeUpdate())
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")

  private val uDataId = prepare("UPDATE TreeEntries SET dataId = ? WHERE id = ?")
  def setDataId(id: Long, dataId: DataId): Unit = synchronized {
    val count = uDataId.sync(_.set(dataId, id).executeUpdate())
    ensure("db.set.dataid", count == 1, s"setDataId update count is $count and not 1 for id $id dataId $dataId")
  }

  private val uRenameMove = prepare("UPDATE TreeEntries SET parentId = ?, name = ? WHERE id = ?")
  /** @return `true` on success, `false` in case of a name conflict.
    * @throws Exception If new parent does not exist or new name is empty. */
  def renameMove(id: Long, newParentId: Long, newName: String): Boolean =
    require(newName.nonEmpty, "Can't rename to an empty name.")
    Try(structureSync {
      // Name conflict or missing parent triggers SQL exception due to unique constraint / foreign key.
      val count = uRenameMove.set(newParentId, newName, id).executeUpdate()
      ensure("db.renameMove", count == 1, s"For id $id, renameMove count is $count instead of 1.")
      count > 0
    }) match
      case Success(value) => value
      case Failure(e: SQLException) if e.getErrorCode == org.h2.api.ErrorCode.DUPLICATE_KEY_1 =>
        log.debug(s"renameMove($id, $newParentId, '$newName'): Name conflict."); false
      case Failure(other) => throw other

  private val dTreeEntry = prepare("UPDATE TreeEntries SET deleted = ? WHERE id = ?")
  /** Deletes a tree entry. Should be called only for existing entry IDs.
    * @return `false` if the tree entry has children. */
  def deleteChildless(id: Long): Boolean = structureSync { // TODO with foreign key relationship, check for exception instead?
    if children(id).nonEmpty then false else
      val count = dTreeEntry.set(now.nonZero, id).executeUpdate()
      ensure("db.delete", count == 1, s"For id $id, delete count is $count instead of 1.")
      count > 0
  }

  private val qNextId = prepare("SELECT NEXT VALUE FOR idSeq")
  private def nextId: Long = qNextId.query(next(_.getLong(1)))
  /** Sets a newly created data ID for a tree entry. */
  def newDataIdFor(id: Long): DataId = DataId(nextId).tap(setDataId(id, _))

  private val iDataEntry = prepare("INSERT INTO DataEntries (id, seq, length, start, stop, hash) VALUES (?, ?, ?, ?, ?, ?)")
  def insertDataEntry(dataId: DataId, seq: Int, length: Long, start: Long, stop: Long, hash: Array[Byte]): Unit =
    ensure("db.add.data.entry.1", seq > 0, s"seq not positive: $seq")
    val sqlLength: Long | SqlNull = if seq == 1 then length else SqlNull(Types.BIGINT)
    val sqlHash: Array[Byte] | SqlNull = if seq == 1 then hash else SqlNull(Types.BINARY)
    val count = iDataEntry.sync(_.set(dataId, seq, sqlLength, start, stop, sqlHash).executeUpdate())
    ensure("db.add.data.entry.2", count == 1, s"insertDataEntry update count is $count and not 1 for dataId $dataId")
