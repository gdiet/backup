package dedup
package db

import org.h2.tools.{RunScript, Script}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.sql.{Connection, Statement}
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

  private def withConnection(dbDir: File)(f: Connection => Any): Unit =
    resource(H2.connection(dbDir, readonly = true))(f)
  private def withStatement(dbDir: File)(f: Statement => Any): Unit =
    withConnection(dbDir)(con => resource(con.createStatement())(f))

  def stats(dbDir: File): Unit = withStatement(dbDir) { stat =>
    log.info(s"Dedup File System Statistics")
    val storageSize = resource(stat.executeQuery("SELECT MAX(stop) FROM DataEntries"))(_.withNext(_.getLong(1)))
    log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d Bytes) / ${stat.executeQuery("SELECT COUNT(id) FROM DataEntries WHERE seq = 1").tap(_.next()).getLong(1)}%,d entries")
    log.info(f"Files: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d")
    log.info(f"Folders: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d")
  }

  def blacklist(dbDir: File, dfsBlacklist: String, deleteCopies: Boolean): Unit = resource(H2.connection(dbDir, readonly = false)) { connection =>
    val db = Database(connection)
    db.child(root.id, dfsBlacklist) match
      case None                          => log.error(s"Can't run blacklisting - DedupFS:/$dfsBlacklist does not exist.")
      case Some(_: FileEntry)            => log.error(s"Can't run blacklisting - DedupFS:/$dfsBlacklist is a file, not a directory.")
      case Some(blacklistRoot: DirEntry) => resource(connection.createStatement()) { stat =>
        def recurse(parentPath: String, parentId: Long): Unit =
          db.children(parentId).foreach {
            case dir: DirEntry =>
              recurse(s"$parentPath/${dir.name}", dir.id)
            case file: FileEntry =>
              val size = stat.executeQuery(s"SELECT stop - start FROM DataEntries WHERE ID = ${file.dataId}")
                .seq(_.getLong(1)).sum
              if size > 0 then
                log.info(s"Blacklisting $parentPath/${file.name}")
                connection.transaction {
                  stat.executeUpdate(s"DELETE FROM DataEntries WHERE id = ${file.dataId} AND seq > 1")
                  stat.executeUpdate(s"UPDATE DataEntries SET start = 0, stop = 0 WHERE id = ${file.dataId}")
                }
              if deleteCopies then
                @annotation.tailrec
                def pathOf(id: Long, pathEnd: String): String =
                  val parentId -> name = stat.executeQuery(s"SELECT parentId, name FROM TreeEntries WHERE id = $id")
                    .one(r => (r.getLong(1), r.getString(2)))
                  val path = s"/$name$pathEnd"
                  if parentId == 0 then path else pathOf(parentId, path)
                val copies = stat.executeQuery(s"SELECT id, parentId, name FROM TreeEntries WHERE dataId = ${file.dataId} AND deleted = 0 AND id != ${file.id}")
                  .seq(r => (r.getLong(1), r.getLong(2), r.getString(3)))
                // TODO why if size == 0 ?
                if size == 0 && copies.nonEmpty then log.info(s"Blacklisting $parentPath/${file.name}")
                copies.foreach { (id, parentId, name) =>
                  log.info(s"Deleting copy in ${pathOf(parentId, "/")}$name")
                  db.delete(id)
                }
          }
        log.info(s"Start blacklisting /$dfsBlacklist")
        recurse(s"/$dfsBlacklist", blacklistRoot.id)
        log.info(s"Finished blacklisting /$dfsBlacklist")
      }
  }

  // TODO at least reclaim 1 & reclaim 2 can be written more readable

  private case class Chunk(start: Long, stop: Long) { def size = stop - start }

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
      val compactionPotential = dataGaps.map(_.size).sum
      log.info(s"Compaction potential of stage 2: ${readableBytes(compactionPotential)} in ${dataGaps.size} gaps.")
    }
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

  def reclaimSpace2(dbDir: File, lts: store.LongTermStore): Unit = withConnection(dbDir) { con =>
    resource(con.createStatement()) { stat =>
      log.info(s"Starting stage 2 of reclaiming space. This modifies the long term store.")
      log.info(s"After this, older database backups can't be fully applied anymore.")

      case class Entry(id: Long, length: Option[Long], hash: Option[Array[Byte]], seq: Int, start: Long, stop: Long)
      val dataEntries: Seq[Entry] = resource(stat.executeQuery("SELECT id, length, hash, seq, start, stop FROM DataEntries"))(
        _.seq(r => Entry(r.getLong(1), r.opt(_.getLong(2)), r.opt(_.getBytes(3)), r.getInt(4), r.getLong(5), r.getLong(6)))
      )
      log.info(s"Number of data entries in storage database: ${dataEntries.size}")
      val dataChunks = dataEntries.map(e => e.start -> e.stop).to(SortedMap)
      log.info(s"Number of data chunks in storage database: ${dataChunks.size}")
      val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
      log.info(s"Current size of data storage: ${readableBytes(endOfStorage)}")
      val compactionPotentialString = readableBytes(dataGaps.map(_.size).sum)
      log.info(s"Compaction potential: $compactionPotentialString in ${dataGaps.size} gaps.")

      case class SortedEntry(id: Long, size: Long, hash: Array[Byte], chunks: Seq[Chunk])
      /** Seq(id, size, hash, Seq(chunk)), ordered by maximum chunk position descending */
      val sortedEntries: Seq[SortedEntry] =
        dataEntries.groupBy(_.id).view.mapValues { entries => // group by id
          val sorted = entries.sortBy(_.seq)
          val Entry(id, Some(size), Some(hash), _, _, _) = sorted.head
          val chunks = sorted.map(e => e.start -> e.stop)
          val chunkSizes = chunks.map(_.size).sum
          require(chunkSizes == 0 || // For blacklisted entries
                  chunkSizes == size, s"Size mismatch for dataId $id: $size is not length of chunks $chunks")
          SortedEntry(id, size, hash, sorted.map(e => Chunk(e.start, e.stop)))
        }.values.toSeq.sortBy(-_.chunks.map(_.start).max) // order by stored last in lts

      val db = Database(con)
      var progressLoggedLast = now.toLong

      @annotation.tailrec
      def reclaim(sortedEntries: Seq[SortedEntry], gaps: Seq[Chunk], reclaimed: Long): Long =
        if sortedEntries.isEmpty then reclaimed else
          if now.toLong > progressLoggedLast + 10000 then
            log.info(s"In progress... reclaimed ${readableBytes(reclaimed)} of $compactionPotentialString.")
            progressLoggedLast = now.toLong
          val SortedEntry(id, entrySize, hash, chunks) = sortedEntries.head
          assert(entrySize > 0, "entry size is zero")
          val (compactionSize, gapsToUse, gapsNotUsed) =
            gaps.foldLeft((0L, Vector.empty[Chunk], Vector.empty[Chunk])) {
              case ((reservedLength, gapsToUse, otherGaps), gap) if reservedLength == entrySize =>
                (reservedLength, gapsToUse, otherGaps.appended(gap))
              case ((reservedLength, gapsToUse, otherGaps), gap) =>
                assert(otherGaps.isEmpty, s"Expected other gaps to be empty but are $otherGaps")
                if reservedLength + gap.size <= entrySize then
                  (reservedLength + gap.size, gapsToUse.appended(gap), otherGaps)
                else
                  val divideAt = gap.start + entrySize - reservedLength
                  (entrySize, gapsToUse.appended(Chunk(gap.start, divideAt)), otherGaps.appended(Chunk(divideAt, gap.stop)))
            }
          if compactionSize == entrySize && gapsToUse.last.stop <= chunks.map(_.start).max then
            assert(entrySize == gapsToUse.map(_.size).sum, s"Size mismatch between entry $entrySize and gaps $gapsToUse")
            log.debug(s"Copying in lts data of entry $id data $chunks to $gapsToUse")
            chunks.foldLeft(gapsToUse) { case (g, c) =>
              @annotation.tailrec
              def copyLoop(gaps: Vector[Chunk], chunk: Chunk): Vector[Chunk] = {
                val gap +: otherGaps = gaps
                val copySize = math.min(memChunk, math.min(gap.size, chunk.size)).toInt
                log.debug(s"COPY at ${chunk.start} size $copySize to ${gap.start}")
                lts.read(chunk.start, copySize, gap.start).foreach(lts.write)
                val restOfGap = Chunk(gap.start + copySize, gap.stop)
                val remainingGaps = if restOfGap.size > 0 then restOfGap +: otherGaps else otherGaps
                val restOfChunk = Chunk(chunk.start + copySize, chunk.stop)
                if restOfChunk.size > 0 then copyLoop(remainingGaps, restOfChunk) else remainingGaps
              }
              copyLoop(g, c)
            }.tap(remainingGaps => require(remainingGaps.isEmpty, s"remaining gaps not empty: $remainingGaps"))
            val newId = DataId(db.nextId)
            con.transaction {
              log.debug(s"Storing in database new data entry $newId for $gapsToUse")
              gapsToUse.zipWithIndex.foreach { case (Chunk(start, stop), index) =>
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
          else
            assert(compactionSize <= entrySize, s"compaction size $compactionSize > entry size $entrySize")
            if compactionSize == entrySize then
              log.info(s"Finished reclaiming: Reordering entry $id would take high effort.")
            else
              log.info(s"Finished reclaiming: Entry $id size ${readableBytes(entrySize)} is larger than remaining compaction potential.")
            reclaimed

      val reclaimed = reclaim(sortedEntries, dataGaps, 0)
      log.info(s"Reclaimed ${readableBytes(reclaimed)}.")
    }
  }
