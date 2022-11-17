package dedup
package db

import java.sql.{Connection, Statement}
import scala.util.Try
import scala.util.Using.resource

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful. Synchronize externally as needed.
final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with util.ClassLogging:
  import connection.prepareStatement as prepare

  def statement[T](f: Statement => T): T = resource(connection.createStatement())(f)

  def shutdownCompact(): Unit =
    log.info("Compacting database...")
    statement(_.execute("SHUTDOWN COMPACT"))

  // Start: Pure read methods that are only used in the write backend
  private val qDataEntry = prepare(
    "SELECT id FROM DataEntries WHERE hash = ? AND length = ?"
  )
  def dataEntry(hash: Array[Byte], size: Long): Option[DataId] = synchronized {
    qDataEntry.set(hash, size).query(maybe(r => DataId(r.getLong(1))))
  }

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
      ensure("data.sort.gaps", false, s"Database might be corrupt. Restore from backup?")
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

  private val iDir = prepare(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS
  )
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = Try {
    // Name conflict triggers SQL exception due to unique constraint.
    val count = iDir.set(parentId, name, now).executeUpdate()
    ensure("db.mkdir", count == 1, s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }.toOption

  private val iFile = prepare(
    "INSERT INTO TreeEntries (parentId, name, time, dataId) VALUES (?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
  )
  /** @return `Some(id)` or [[None]] if a child entry with the same name already exists. */
  def mkFile(parentId: Long, name: String, time: Time, dataId: DataId): Option[Long] = Try {
    // Name conflict triggers SQL exception due to unique constraint.
    val count = iFile.set(parentId, name, time, dataId).executeUpdate()
    ensure("db.mkfile", count == 1, s"For parentId $parentId and name '$name', mkFile update count is $count instead of 1.")
    iFile.getGeneratedKeys.tap(_.next()).getLong("id")
  }.toOption

  private val uTime = prepare(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit =
    val count = uTime.set(newTime, id).executeUpdate()
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")

  private val uDataId = prepare(
    "UPDATE TreeEntries SET dataId = ? WHERE id = ?"
  )
  def setDataId(id: Long, dataId: DataId): Unit = synchronized {
    val count = uDataId.set(dataId, id).executeUpdate()
    ensure("db.set.dataid", count == 1, s"setDataId update count is $count and not 1 for id $id dataId $dataId")
  }

  private val dTreeEntry = prepare(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  /** Deletes a tree entry. Should be called only for existing entry IDs. */
  def delete(id: Long): Unit =
    val count = dTreeEntry.set(now.nonZero, id).executeUpdate()
    ensure("db.delete", count == 1, s"For id $id, delete count is $count instead of 1.")
