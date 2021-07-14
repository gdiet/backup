package dedup
package db

import org.h2.tools.{RunScript, Script}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.SortedMap
import scala.util.Using.resource

object maintenance extends util.ClassLogging:

  def backup(dbDir: File): Unit =
    val file = File(dbDir, "dedupfs.mv.db")
    require(file.exists(), s"Database file $file doesn't exist")
    val plainBackup = File(dbDir, "dedupfs.mv.db.backup")
    log.info(s"Creating plain database backup: $file -> $plainBackup")
    Files.copy(file.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

    val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
    val zipBackup = File(dbDir, s"dedupfs_$dateString.zip")
    log.info(s"Creating sql script database backup: $file -> $zipBackup")
    Script.main(
      "-url", s"jdbc:h2:$dbDir/dedupfs", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
    )

  def restoreBackup(dbDir: File, from: Option[String]): Unit = from match
    case None =>
      val dbFile = File(dbDir, "dedupfs.mv.db")
      val backup = File(dbDir, "dedupfs.mv.db.backup")
      require(backup.exists(), s"Database backup file $backup doesn't exist")
      log.info(s"Restoring plain database backup: $backup -> $dbFile")
      Files.copy(backup.toPath, dbFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

    case Some(scriptName) =>
      val script = File(dbDir, scriptName)
      require(script.exists(), s"Database backup script file $script doesn't exist")
      val dbFile = File(dbDir, "dedupfs.mv.db")
      require(!dbFile.exists || dbFile.delete, s"Can't delete current database file $dbFile")
      RunScript.main(
        "-url", s"jdbc:h2:$dbDir/dedupfs", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
      )

  private def withStatement(dbDir: File)(f: Statement => Any): Unit =
    resource(H2.connection(dbDir, readonly = true))(con => resource(con.createStatement())(f))

  def stats(dbDir: File): Unit = withStatement(dbDir) { stat =>
    log.info(s"Dedup File System Statistics")
    val storageSize = resource(stat.executeQuery("SELECT MAX(stop) FROM DataEntries"))(_.withNext(_.getLong(1)))
    log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d Bytes) / ${stat.executeQuery("SELECT COUNT(id) FROM DataEntries WHERE seq = 1").tap(_.next()).getLong(1)}%,d entries")
    log.info(f"Files: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d")
    log.info(f"Folders: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d")
  }

  def reclaimSpace1(dbDir: File, keepDeletedDays: Int): Unit = withStatement(dbDir) { stat =>
    log.info(s"Starting stage 1 of reclaiming space. Undo by restoring the database from a backup.")
    log.info(s"Note that stage 2 of reclaiming space modifies the long term store")
    log.info(s"  thus partially invalidates older database backups.")

    log.info(s"Deleting tree entries marked for deletion more than $keepDeletedDays days ago...")
    val deleteBefore = now.toLong - keepDeletedDays*24*60*60*1000
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
      val dataIdsInTree = resource(stat.executeQuery(
        "SELECT DISTINCT(dataId) FROM TreeEntries WHERE dataId >= 0"
      ))(_.seq(_.getLong(1))).toSet
      log.info(s"Number of data entries found in tree database: ${dataIdsInTree.size}")
      val dataIdsInStorage = stat.executeQuery("SELECT id FROM DataEntries").seq(_.getLong(1)).toSet
      log.info(s"Number of data entries in storage database: ${dataIdsInStorage.size}")
      val dataIdsToDelete = dataIdsInStorage -- dataIdsInTree
      dataIdsToDelete.foreach(dataId => stat.executeUpdate(
        s"DELETE FROM DataEntries WHERE id = $dataId"
      ))
      log.info(s"Number of orphan data entries deleted from storage database: ${dataIdsToDelete.size}")
      val orphanDataIdsInTree = (dataIdsInTree -- dataIdsInStorage).size
      if orphanDataIdsInTree > 0 then log.warn(s"Number of orphan data entries found in tree database: $orphanDataIdsInTree")
    }

    { // Run in separate block so the possibly large collections can be garbage collected soon
      log.info(s"Checking compaction potential of the data entries:")
      val dataChunks = resource(stat.executeQuery(
        "SELECT start, stop FROM DataEntries"
      ))(_.seq(r => r.getLong(1) -> r.getLong(2))).to(SortedMap)
      log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
      val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
      log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
      val compactionPotential = combinedSize(dataGaps)
      log.info(s"Compaction potential of stage 2: ${readableBytes(compactionPotential)} in ${dataGaps.size} gaps.")
    }
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

  private def combinedSize(chunks: Seq[(Long, Long)]): Long =
    chunks.map { case (start, stop) => stop - start }.sum

  private def endOfStorageAndDataGaps(dataChunks: SortedMap[Long, Long]): (Long, Seq[(Long, Long)]) =
    dataChunks.foldLeft(0L -> Vector.empty[(Long, Long)]) {
      case ((lastEnd, gaps), (start, stop)) if start < lastEnd =>
        log.warn(s"Detected overlapping data entry ($start, $stop).")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) if start == lastEnd =>
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(lastEnd -> start)
    }
