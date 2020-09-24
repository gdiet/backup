package dedup

import java.lang.System.{currentTimeMillis => now}
import java.sql.Connection

import dedup.Database._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.SortedMap
import scala.util.Using.resource

object DBMaintenance {
  implicit private val log: Logger = LoggerFactory.getLogger("dedup.DButl")

  def endOfStorageAndDataGaps(dataChunks: SortedMap[Long, Long]): (Long, Seq[(Long, Long)]) = {
    dataChunks.foldLeft(0L -> Vector.empty[(Long, Long)]) {
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
    log.info(s"Note that stage 2 of reclaiming space will modify the long term store")
    log.info(s"thus partially invalidates database backups.")

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
      log.info(f"Current size of data storage: ${endOfStorage/1000000000d}%,.2f GB")
      val compactionPotential = dataGaps.map{ case (start, stop) => stop - start }.sum
      log.info(f"Compaction potential of stage 2: ${compactionPotential/1000000000d}%,.2f GB in ${dataGaps.size} gaps.")
    }
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

  def reclaimSpace2(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
    log.info(s"Starting stage 2 of reclaiming space. This modifies the long term store.")
    log.info(s"After this, database backups can't be fully applied anymore.")

    val dataEntries = allDataEntries(stat)
    log.info(s"Number of data entries in storage database: ${dataEntries.size}")
    val dataChunks = dataEntries.map(e => e._3 -> e._4).to(SortedMap)
    log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
    val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
    log.info(f"Current size of data storage: ${endOfStorage/1000000000d}%,.2f GB")
    val compactionPotential = dataGaps.map{ case (start, stop) => stop - start }.sum
    log.info(f"Compaction potential: ${compactionPotential/1000000000d}%,.2f GB in ${dataGaps.size} gaps.")

    {
      val sortedEntries =
        dataEntries
          .groupBy(_._1)                                  // group by id
          .view.mapValues { entries =>
            entries.head._1 ->                            // id -> ..
              entries.sortBy(_._2).map(e => e._3 -> e._4) // .. -> Seq(start, stop) ordered by seq
          }
          .values.toSeq
          .sortBy(_._2.map(_._1).max)                     // order by stored last in lts
      val maybeEntry = sortedEntries.lastOption
      val remainingEntries = sortedEntries.dropRight(1)

      maybeEntry.foreach { case (id, chunks) =>
      }
    }
//    log.info(s"Reading data entries:")
//    val chunks =
//      stat.executeQuery("SELECT start, stop FROM DataEntries").seq(r => r.getLong(1) -> r.getLong(2)).to(SortedMap)
//    log.info(s"Read all ${chunks.size} data entries.")
//    val (_, dataGaps) = chunks.foldLeft(0L -> Vector.empty[(Long, Long)]) {
//      case ((lastEnd, gaps), (start, stop)) if start < lastEnd =>
//        log.warn(s"Detected overlapping data entry ($start, $stop).")
//        stop -> gaps
//      case ((lastEnd, gaps), (start, stop)) if start == lastEnd =>
//        stop -> gaps
//      case ((lastEnd, gaps), (start, stop)) =>
//        stop -> gaps.appended(lastEnd -> start)
//    }
//    val compactionPotential = dataGaps.map{ case (start, stop) => stop - start }.sum
//    log.info(f"Compaction potential: ${compactionPotential/1000000000d}%,.2f GB in ${dataGaps.size} gaps.")
//
//    // tail recursive method, each call one move
//    // if chunkToMove.start < gapsToUse.max(stop) then don't
//
//    val (start, stop) :: tail = chunks.toList.reverse
//    val length = stop - start
//
//    val (_, gapsToUse, remainingGaps) = dataGaps.foldLeft((0L, Vector[(Long, Long)](), Vector[(Long, Long)]())) {
//      case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) if availableLength == length =>
//        (availableLength, gapsToUse, remainingGaps.appended(cStart -> cStop))
//      case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) =>
//        val cLength = cStop - cStart
//        if (availableLength + cLength <= length)
//          (availableLength + cLength, gapsToUse.appended(cStart -> cStop), remainingGaps)
//        else {
//          val divideAt = cStart + length - availableLength
//          (length, gapsToUse.appended(cStart -> divideAt), remainingGaps.appended(divideAt -> cStop))
//        }
//    }
  }
}
