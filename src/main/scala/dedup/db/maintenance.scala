package dedup
package db

import dedup.db.H2.dbName
import org.h2.tools.{RunScript, Script}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date

object maintenance extends util.ClassLogging:

  def backup(dbDir: File, fileNameSuffix: String = ""): Unit =
    val dbFile = H2.dbFile(dbDir)
    ensure("tool.backup", dbFile.exists(), s"Database file $dbFile does not exist")
    val plainBackup = H2.dbFile(dbDir, ".backup")
    log.info(s"Creating plain database backup: ${dbFile.getName} -> ${plainBackup.getName}")
    Files.copy(dbFile.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

    val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
    val zipBackup = File(dbDir, s"dedupfs_$dateString$fileNameSuffix.zip")
    log.info(s"Creating sql script database backup: ${dbFile.getName} -> ${zipBackup.getName}")
    log.info(s"To restore the database, run 'db-restore ${zipBackup.getName}'.")
    Script.main(
      "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
    )

  def restorePlainBackup(dbDir: File): Unit =
    val dbFile = H2.dbFile(dbDir)
    val plainBackup = H2.dbFile(dbDir, ".backup")
    ensure("tool.restore.notfound", plainBackup.exists(), s"Database backup file $plainBackup does not exist")
    log.info(s"Restoring plain database backup: ${plainBackup.getName} -> ${dbFile.getName}")
    Files.copy(plainBackup.toPath, dbFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

  def restoreScriptBackup(dbDir: File, scriptName: String): Unit =
    val script = File(dbDir, scriptName)
    ensure("tool.restore.from", script.exists(), s"Database backup script file $script does not exist")
    val dbFile = H2.dbFile(dbDir)
    ensure("tool.restore", !dbFile.exists || dbFile.delete, s"Can't delete current database file $dbFile")
    log.info(s"Restoring database backup: ${script.getName} -> ${dbFile.getName}")
    RunScript.main(
      "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
    )

  def compactDb(dbDir: File): Unit = withDb(dbDir, readonly = false)(_.shutdownCompact())
  
  def stats(dbDir: File): Unit = withDb(dbDir) { db =>
    import Database.currentDbVersion
    log.info(s"Dedup File System Statistics")
    db.version() match
      case None => log.error("No database version available.")
      case Some(`currentDbVersion`) => log.info(s"Database version $currentDbVersion - OK.")
      case Some(otherDbVersion) => log.warn(s"Database version $otherDbVersion is INCOMPATIBLE, expected $currentDbVersion.")
    val storageSize = db.storageSize()
    log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d bytes) / ${db.countDataEntries()}%,d entries")
    log.info(f"Files: ${db.countFiles()}%,d, deleted ${db.countDeletedFiles()}%,d")
    log.info(f"Folders: ${db.countDirs()}%,d, deleted ${db.countDeletedDirs()}%,d")
    log.info("Checking compaction potential of the data storage...")
    db.freeAreas() // Run for its log output
  }

  def list(dbDir: File, path: String): Unit = withDb(dbDir) { db =>
    db.entry(path) match
      case None =>
        println(s"The path '$path' does not exist.")
      case Some(file: FileEntry) =>
        println(s"File information for path '$path':")
        println(s"${file.name} .. ${readableBytes(db.dataSize(file.dataId))}")
      case Some(dir: DirEntry) =>
        println(s"Listing of directory '$path':")
        db.children(dir.id).sortBy {
          case dir: DirEntry => s"d${dir.name}"
          case file: FileEntry => s"f${file.name}"
        }.foreach {
          case dir: DirEntry => println(s"> ${dir.name}")
          case file: FileEntry => println(s"- ${file.name} ${"." * math.max(2, 38-file.name.length)} ${readableBytes(db.dataSize(file.dataId))}")
        }
  }

  def del(dbDir: File, path: String): Unit = withDb(dbDir, readonly = false) { db =>
    db.entry(path) match
      case None =>
        println(s"The path '$path' does not exist.")
      case Some(file: FileEntry) =>
        if db.deleteChildless(file.id)
        then log.info(s"Marked deleted file '$path' .. ${readableBytes(db.dataSize(file.dataId))}")
        else log.warn(s"Could not delete file with children: '$path'")
      case Some(dir: DirEntry) =>
        log.info(s"Marking deleted directory '$path' ...")
        def delete(treeEntry: TreeEntry): Long =
          val childCount = db.children(treeEntry.id).map(delete).sum
          if db.deleteChildless(treeEntry.id)
          then log.debug(s"Marked deleted: $treeEntry")
          else log.warn(s"Could not delete file with children: '$path'")
          childCount + 1
        log.info(s"Marked deleted ${delete(dir)} files/directories.")
  }

  def find(dbDir: File, nameLike: String): Unit = withDb(dbDir) { db =>
    println(s"Searching for files matching '$nameLike':")

    @annotation.tailrec
    def path(entry: TreeEntry, acc: List[TreeEntry] = Nil): List[TreeEntry] =
      if entry.id == root.id then entry :: acc else
        db.entry(entry.parentId) match // fold can not be used tail recursively
          case None => Nil
          case Some(parent) => path(parent, entry :: acc)

    db.entryLike(nameLike).foreach { entry =>
      path(entry) match
        case Nil => /* deleted entry */
        case entries =>
          println(entries.map {
            case dirEntry: DirEntry => s"${dirEntry.name}/"
            case fileEntry: FileEntry => fileEntry.name
          }.mkString)
    }
  }

  def reclaimSpace(dbDir: File, keepDeletedDays: Int): Unit = withDb(dbDir, readonly = false) { db =>
    log.info(s"Reclaiming space from deleted files and orphan data entries now.")

    log.info(s"Deleting tree entries marked for deletion more than $keepDeletedDays days ago...")

    // First un-root, then delete. Deleting directly can violate the foreign key constraint.
    log.info(s"Part 1: Mark the tree entries to delete...")
    log.info(s"Number of entries marked: ${db.unrootDeletedEntries(now.asLong - keepDeletedDays*24*60*60*1000)}")
    log.info(s"Part 2: Deleting marked tree entries...")
    log.info(s"Number of tree entries deleted: ${db.deleteUnrootedTreeEntries()}")

    // Note: Most operations implemented in Scala below could also be run in SQL, but that is much slower...

    { // Run in separate block so the possibly large collections can be garbage collected soon
      log.info(s"Deleting orphan data entries...")
      val dataIdsInTree = db.dataIdsInTree()
      log.info(s"Number of data entries found in tree database: ${dataIdsInTree.size}")
      val dataIdsInStorage = db.dataIdsInStorage()
      log.info(s"Number of data entries in storage database: ${dataIdsInStorage.size}")
      val dataIdsToDelete = dataIdsInStorage -- dataIdsInTree
      dataIdsToDelete.foreach(db.deleteDataEntry)
      log.info(s"Number of orphan data entries deleted: ${dataIdsToDelete.size}")
      val orphanDataIdsInTree = (dataIdsInTree -- dataIdsInStorage).size
      if orphanDataIdsInTree > 0 then log.warn(s"Number of orphan data entries found in tree database: $orphanDataIdsInTree")
    }

    log.info("Checking compaction potential of the data storage:")
    db.freeAreas() // Run for its log output

    db.shutdownCompact()
    log.info("Finished reclaiming space. Undo by restoring the database from a backup.")
    log.info("Note: Once new files are stored, restoring a database backup from before")
    log.info("        the reclaim process will result in partial data corruption.")
  }
