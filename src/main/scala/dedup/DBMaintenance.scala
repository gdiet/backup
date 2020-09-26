package dedup

import java.lang.System.{currentTimeMillis => now}
import java.sql.Connection

import dedup.Database._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.SortedMap
import scala.util.Using.resource

object DBMaintenance {
  implicit private val log: Logger = LoggerFactory.getLogger("dedup.DButl")

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
      log.info(s"Current size of data storage: ${readable(endOfStorage)}B")
      val compactionPotential = combinedSize(dataGaps)
      log.info(s"Compaction potential of stage 2: ${readable(compactionPotential)}B in ${dataGaps.size} gaps.")
    }
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

  def reclaimSpace2(connection: Connection): Unit = resource(connection.createStatement()) { stat =>
    log.info(s"Starting stage 2 of reclaiming space. This modifies the long term store.")
    log.info(s"After this, database backups can't be fully applied anymore.")

    val dataEntries = allDataEntries(stat)
    log.info(s"Number of data entries in storage database: ${dataEntries.size}")
    val dataChunks = dataEntries.map(e => e._5 -> e._6).to(SortedMap)
    log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
    val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
    log.info(s"Current size of data storage: ${readable(endOfStorage)}B")
    val compactionPotentialString = readable(combinedSize(dataGaps))+"B"
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

    @annotation.tailrec // FIXME return actual compaction
    def reclaim(sortedEntries: Seq[Entry], gaps: Seq[Chunk], reclaimed: Long): Long =
      if (sortedEntries.isEmpty) reclaimed else { // can't fold or foreach because that's not tail recursive in Scala 2.13.3
        if (now > progressLoggedLast + 10000) {
          log.info(s"In progress... reclaimed ${readable(reclaimed)}B of $compactionPotentialString.")
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
          log.debug(s"Copying in lts data of entry $id to $gapsToUse")
          Thread.sleep(100) // FIXME store in lts data
          val newId = db.nextId
          connection.transaction {
            log.debug(s"Storing in database new data entry $newId for $gapsToUse")
            gapsToUse.zipWithIndex.foreach { case ((start, stop), index) =>
              db.insertDataEntry(newId, index + 1, entrySize, start, stop, hash) // FIXME add hash
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
          assert(compactionSize < entrySize, s"compaction size $compactionSize > entry size $entrySize")
          reclaimed
        }
      }

    val reclaimed = reclaim(sortedEntries, dataGaps, 0)
    log.info(s"Reclaimed ${readable(reclaimed)}B.")
  }
}
