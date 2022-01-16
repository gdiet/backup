package dedup
package maintain

import java.io.File
import java.sql.{Connection, Statement}
import scala.collection.SortedMap
import scala.util.Using.resource

import db.query

object reclaim extends util.ClassLogging:
  import db.maintenance.{withConnection, withStatement}
  import db.{Database, maybeNext, opt, seq, transaction}

  // TODO reclaimSpace can probably be written more readable + testable

  private case class Chunk(start: Long, stop: Long) { def size: Long = stop - start }

  def reclaimSpace(dbDir: File, keepDeletedDays: Int): Unit = withStatement(dbDir, readonly = false) { stat =>
    log.info(s"Starting stage 1 of reclaiming space. Undo by restoring the database from a backup.")
    log.info(s"Note that stage 2 of reclaiming space modifies the long term store")
    log.info(s"  thus partially invalidates older database backups.")

    log.info(s"Deleting tree entries marked for deletion more than $keepDeletedDays days ago...")
    val deleteBefore = now.toLong - keepDeletedDays*24*60*60*1000

    // First un-root, then delete. Deleting directly can violate the foreign key constraint.
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
      val dataIdsInTree = stat.query(
        "SELECT DISTINCT(dataId) FROM TreeEntries WHERE dataId >= 0"
      )(_.seq(_.getLong(1))).toSet
      log.info(s"Number of data entries found in tree database: ${dataIdsInTree.size}")
      val dataIdsInStorage = stat.query("SELECT id FROM DataEntries")(_.seq(_.getLong(1))).toSet
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
      val dataChunks = stat.query(
        "SELECT start, stop FROM DataEntries"
      )(_.seq(r => r.getLong(1) -> r.getLong(2)))
      log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
      val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks.to(SortedMap))
      log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
      val compactionPotential = dataGaps.map(_.size).sum
      log.info(s"Compaction potential of stage 2: ${readableBytes(compactionPotential)} in ${dataGaps.size} gaps.")
    }
    log.info(s"Compacting database...")
    stat.execute("SHUTDOWN COMPACT;")
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

  private def endOfStorageAndDataGaps(dataChunks: SortedMap[Long, Long]): (Long, Seq[Chunk]) =
    dataChunks.foldLeft(0L -> Vector.empty[Chunk]) {
      case ((lastEnd, gaps), (start, stop)) if start < lastEnd =>
        log.warn(s"Detected overlapping data entry ($start, $stop).")
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) if start == lastEnd =>
        stop -> gaps
      case ((lastEnd, gaps), (start, stop)) =>
        stop -> gaps.appended(Chunk(lastEnd, start))
    }
