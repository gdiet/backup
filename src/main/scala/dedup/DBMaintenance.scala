package dedup

import dedup.Database._
import dedup.store.LongTermStore
import org.h2.tools.{RunScript, Script}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.SortedMap
import scala.util.Using.resource

object DBMaintenance {
  implicit private val log: Logger = LoggerFactory.getLogger("dedup.dbUtl")

  type Chunk = (Long, Long)
  implicit class ChunkDecorator(chunk: Chunk) {
    def start: Long = chunk._1
    def stop: Long = chunk._2
    def size: Long = chunk.pipe { case (start, stop) => stop - start }
  }
  def combinedSize(chunks: Seq[Chunk]): Long = chunks.map(_.size).sum

  def createBackup(repo: File): Unit = {
    val dbDir = Database.dbDir(repo).getAbsoluteFile
    val dbFile = new File(dbDir, "dedupfs.mv.db")
    require(dbFile.exists(), s"Database file $dbFile doesn't exist")
    val backup = new File(dbDir, "dedupfs.mv.db.backup")
    log.info(s"Creating plain database backup: $dbFile -> $backup")
    Files.copy(dbFile.toPath, backup.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

    val dateString = new SimpleDateFormat("yyyy-MM-dd_HH-mm").format(new Date())
    val target = new File(dbDir, s"dedupfs_$dateString.zip")
    log.info(s"Creating sql script database backup: $dbFile -> $target")
    Script.main(
      "-url", s"jdbc:h2:$dbDir/dedupfs", "-script", s"$target", "-user", "sa", "-options", "compression", "zip"
    )
  }

  def restoreBackup(repo: File, from: Option[String]): Unit = from match {
    case None =>
      val dbDir = Database.dbDir(repo).getAbsoluteFile
      val dbFile = new File(dbDir, "dedupfs.mv.db")
      val backup = new File(dbDir, "dedupfs.mv.db.backup")
      require(backup.exists(), s"Database backup file $backup doesn't exist")
      log.info(s"Restoring plain database backup: $backup -> $dbFile")
      Files.copy(backup.toPath, dbFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

    case Some(scriptFilePath) =>
      val script = new File(scriptFilePath).getAbsoluteFile
      require(script.exists(), s"Database backup script file $script doesn't exist")
      def scriptRepo = script.getCanonicalFile.getParentFile.getParentFile
      require(scriptRepo == repo.getCanonicalFile, s"Database backup script file $script is not stored in repo $repo")
      val dbDir = Database.dbDir(repo).getAbsoluteFile
      val dbFile = new File(dbDir, "dedupfs.mv.db")
      require(!dbFile.exists || dbFile.delete, s"Can't delete current database file $dbFile")
      RunScript.main(
        "-url", s"jdbc:h2:$dbDir/dedupfs", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
      )
  }

  def endOfStorageAndDataGaps(dataChunks: SortedMap[Long, Long]): (Long, Seq[Chunk]) = {
    dataChunks.foldLeft(0L -> Vector.empty[Chunk]) {
      case ((lastEnd, gaps), (start, stop)) if start < lastEnd =>
        log.warn(s"Detected overlapping data entry ($start, $stop).")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) if start == lastEnd =>
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(lastEnd -> start)
    }
  }

  def reclaimSpace1(connection: Connection, keepDeletedDays: Int): Unit = resource(connection.createStatement()) { stat =>
    log.info(s"Starting stage 1 of reclaiming space. Undo by restoring the database from a backup.")
    log.info(s"Note that stage 2 of reclaiming space modifies the long term store")
    log.info(s"  thus partially invalidates older database backups.")

    log.info(s"Deleting tree entries marked for deletion more than $keepDeletedDays days ago...")
    val deleteBefore = now - keepDeletedDays*1000*3600*24
    log.info(s"Part 1: Un-rooting the tree entries to delete...")
    val entriesUnrooted = stat.executeUpdate(
      s"UPDATE TreeEntries SET parentId = id WHERE deleted != 0 AND deleted < $deleteBefore"
    )
    log.info(s"Number of entries un-rooted: $entriesUnrooted")

    log.info(s"Part 2: Deleting un-rooted tree entries...")
    val treeEntriesDeleted = stat.executeUpdate(
      s"DELETE FROM TreeEntries WHERE id = parentId AND id != 0"
    )
    log.info(s"Number of un-rooted tree entries deleted: $treeEntriesDeleted")

    // Note: Most operations implemented in Scala below could also be run in SQL, but that is much slower...

    { // Run in separate block so the possibly large collections can be garbage collected soon
      log.info(s"Deleting orphan data entries from storage database...")
      // Note: The WHERE clause also makes sure the 'null' entries are not returned
      val dataIdsInTree =
        stat.executeQuery("SELECT DISTINCT(dataId) FROM TreeEntries WHERE dataId >= 0").seq(_.getLong(1)).toSet
      log.info(s"Number of data entries found in tree database: ${dataIdsInTree.size}")
      val dataIdsInStorage = stat.executeQuery("SELECT id FROM DataEntries").seq(_.getLong(1)).toSet
      log.info(s"Number of data entries in storage database: ${dataIdsInStorage.size}")
      val dataIdsToDelete = dataIdsInStorage -- dataIdsInTree
      dataIdsToDelete.foreach(dataId =>
        stat.executeUpdate(s"DELETE FROM DataEntries WHERE id = $dataId")
      )
      log.info(s"Number of orphan data entries deleted from storage database: ${dataIdsToDelete.size}")
      val orphanDataIdsInTree = (dataIdsInTree -- dataIdsInStorage).size
      if (orphanDataIdsInTree > 0)
        log.warn(s"Number of orphan data entries found in tree database: $orphanDataIdsInTree")
    }

    { // Run in separate block so the possibly large collections can be garbage collected soon
      log.info(s"Checking compaction potential of the data entries:")
      val dataChunks = allDataChunks(stat).to(SortedMap)
      log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
      val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
      log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
      val compactionPotential = combinedSize(dataGaps)
      log.info(s"Compaction potential of stage 2: ${readableBytes(compactionPotential)} in ${dataGaps.size} gaps.")
    }
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

  def reclaimSpace2(connection: Connection, lts: LongTermStore): Unit = resource(connection.createStatement()) { stat =>
    log.info(s"Starting stage 2 of reclaiming space. This modifies the long term store.")
    log.info(s"After this, older database backups can't be fully applied anymore.")

    val dataEntries = allDataEntries(stat)
    log.info(s"Number of data entries in storage database: ${dataEntries.size}")
    val dataChunks = dataEntries.map(e => e._5 -> e._6).to(SortedMap)
    log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
    val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
    log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
    val compactionPotentialString = readableBytes(combinedSize(dataGaps))
    log.info(s"Compaction potential: $compactionPotentialString in ${dataGaps.size} gaps.")

    type Entry = (Long, Long, Array[Byte], Seq[Chunk])
    /** Seq(dataId -> Seq(chunk)), ordered by maximum chunk position descending */
    val sortedEntries: Seq[Entry] =
      dataEntries.groupBy(_._1).view.mapValues { entries => // group by id
        val sorted = entries.sortBy(_._4)
        val (id, Some(size), Some(hash), _, _, _) = sorted.head
        val chunks = sorted.map(e => e._5 -> e._6)
        assert(combinedSize(chunks) == size, s"Size mismatch for dataId $id: $size is not length of chunks $chunks")
        (id, size, hash, sorted.map(e => e._5 -> e._6))
      }.values.toSeq.sortBy(-_._4.map(_.start).max) // order by stored last in lts

    val db = new Database(connection)
    var progressLoggedLast = now

    @annotation.tailrec
    def reclaim(sortedEntries: Seq[Entry], gaps: Seq[Chunk], reclaimed: Long): Long =
      if (sortedEntries.isEmpty) reclaimed else { // can't fold or foreach because that's not tail recursive in Scala 2.13.3
        if (now > progressLoggedLast + 10000) {
          log.info(s"In progress... reclaimed ${readableBytes(reclaimed)} of $compactionPotentialString.")
          progressLoggedLast = now
        }

        val (id, entrySize, hash, chunks) = sortedEntries.head
        assert(entrySize > 0, "entry size is zero")
        val (compactionSize, gapsToUse, gapsNotUsed) =
          gaps.foldLeft((0L, Vector.empty[Chunk], Vector.empty[Chunk])) {
            case ((reservedLength, gapsToUse, otherGaps), gap) if reservedLength == entrySize =>
              (reservedLength, gapsToUse, otherGaps.appended(gap))
            case ((reservedLength, gapsToUse, otherGaps), gap) =>
              assert(otherGaps.isEmpty, s"Expected other gaps to be empty but are $otherGaps")
              if (reservedLength + gap.size <= entrySize)
                (reservedLength + gap.size, gapsToUse.appended(gap), otherGaps)
              else {
                val divideAt = gap.start + entrySize - reservedLength
                (entrySize, gapsToUse.appended(gap.start -> divideAt), otherGaps.appended(divideAt -> gap.stop))
              }
          }
        if (compactionSize == entrySize && gapsToUse.last.stop <= chunks.map(_.start).max) {
          assert(entrySize == combinedSize(gapsToUse), s"Size mismatch between entry $entrySize and gaps $gapsToUse")
          log.debug(s"Copying in lts data of entry $id data $chunks to $gapsToUse")
          chunks.foldLeft(gapsToUse) { case (g, c) =>
            @annotation.tailrec
            def copyLoop(gaps: Vector[Chunk], chunk: Chunk): Vector[Chunk] = {
              val gap +: otherGaps = gaps
              val copySize = math.min(memChunk, math.min(gap.size, chunk.size)).toInt
              log.debug(s"COPY at ${chunk.start} size $copySize to ${gap.start}")
              lts.write(gap.start, lts.read(chunk.start, copySize))
              val restOfGap = (gap.start + copySize, gap.stop)
              val remainingGaps = if (restOfGap.size > 0) restOfGap +: otherGaps else otherGaps
              val restOfChunk = (chunk.start + copySize, chunk.stop)
              if (restOfChunk.size > 0) copyLoop(remainingGaps, restOfChunk) else remainingGaps
            }
            copyLoop(g, c)
          }.tap(remainingGaps => require(remainingGaps.isEmpty, s"remaining gaps not empty: $remainingGaps"))
          val newId = db.nextId
          connection.transaction {
            log.debug(s"Storing in database new data entry $newId for $gapsToUse")
            gapsToUse.zipWithIndex.foreach { case ((start, stop), index) =>
              db.insertDataEntry(newId, index + 1, entrySize, start, stop, hash)
            }
            log.debug(s"Replacing old data entry $id with new data entry $newId in tree entries")
            require(stat.executeUpdate(s"UPDATE TreeEntries SET dataId = $newId WHERE dataId = $id") > 0,
              s"No tree entry found to replace data entry $id in.")
            log.debug(s"Deleting old data entry $id from storage database.")
            require(stat.executeUpdate(s"DELETE FROM DataEntries WHERE id = $id") > 0,
              s"No data entry $id found when trying to delete it.")
          }
          reclaim(sortedEntries.drop(1), gapsNotUsed, reclaimed + compactionSize)
        } else {
          assert(compactionSize <= entrySize, s"compaction size $compactionSize > entry size $entrySize")
          if (compactionSize == entrySize)
            log.info(s"Finished reclaiming: Reordering entry $id would take high effort.")
          else
            log.info(s"Finished reclaiming: Entry $id size ${readableBytes(entrySize)} is larger than remaining compaction potential.")
          reclaimed
        }
      }

    val reclaimed = reclaim(sortedEntries, dataGaps, 0)
    log.info(s"Reclaimed ${readableBytes(reclaimed)}.")
  }
}
