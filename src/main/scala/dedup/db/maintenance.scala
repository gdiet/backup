package dedup
package db

import dedup.db.H2.{dbFile, dbName, backupFile, backupName, previousDbFile}
import org.h2.tools.{RunScript, Script}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.CountDownLatch

object maintenance extends util.ClassLogging:

  /** Create an SQL backup from the previous version's database file,
   *  then rename the file to "before_upgrade".
   *
   *  This method assumes that the right database driver for the previous
   *  version's database is in the classpath. */
  def migrateDbStep1(dbDir: File): Unit =
    ensure("maintenance.migrateDB.step1", ! dbFile(dbDir).exists(),
      s"Can't migrate old database - a new database is already present: ${dbFile(dbDir)}")
    val fileToMigrate = previousDbFile(dbDir)
    val awaitable = sqlDbBackup(dbDir, fileToMigrate, "_before_upgrade")
    awaitable.await()
    val updatedFile = File(dbDir, H2.previousDbName + "_before_upgrade.mv.db")
    ensure("maintenance.migrateDB.step1", fileToMigrate.renameTo(updatedFile),
      s"Could not rename $fileToMigrate to $updatedFile.")

  /** Restore the database from the file created in step 1,
   *  this time with the current database driver. */
  def migrateDbStep2(dbDir: File): Unit =
    val pattern = s"${H2.previousDbName}_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}_before_upgrade.zip"
    val backupCandidates = dbDir.listFiles().filter(_.getName.matches(pattern))
    log.info("xx " + pattern)
    log.info("xx " + dbDir.listFiles().toList)
    ensure("maintenance.migrateDB.step2", backupCandidates.length == 1,
      s"Found no or too many DB backup candidates for migration step 2: ${backupCandidates.toList}")
    restoreSqlDbBackup(dbDir, backupCandidates(0).getName)

  /** @return A CountDownLatch that becomes available when the SQL backup is complete. */
  def dbBackup(dbDir: File, fileNameSuffix: String = ""): CountDownLatch =
    val plainBackup = backupFile(dbDir)
    plainDbBackup(dbDir, plainBackup)
    sqlDbBackup(dbDir, plainBackup, fileNameSuffix)

  private def plainDbBackup(dbDir: File, plainBackup: File): Unit =
    val database = dbFile(dbDir)
    ensure("utility.backup", database.exists(), s"Database file $database does not exist")
    log.info(s"Creating plain database backup: ${database.getName} -> ${plainBackup.getName}")
    Files.copy(database.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

  /** @return A CountDownLatch that becomes available when the SQL backup is complete. */
  private def sqlDbBackup(dbDir: File, dbFile: File, fileNameSuffix: String): CountDownLatch =
    ensure("maintenance.sqlBackup", dbFile.getName.endsWith(".mv.db"),
      s"Name of database file to back up $dbFile does not end in '.mv.db'.")
    ensure("maintenance.sqlBackup", dbFile.getParentFile == dbDir,
      s"Database file to back up $dbFile is not in database directory $dbDir.")
    val awaitable = java.util.concurrent.CountDownLatch(1)
    new Thread(() => {
      try
        val dbPathWithoutExtension = dbFile.getPath.dropRight(6)
        cache.MemCache.availableMem.addAndGet(-64000000) // Reserve some RAM for the backup process
        val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
        val zipBackup = File(s"${dbPathWithoutExtension}_$dateString$fileNameSuffix.zip")
        log.info(s"Creating sql script database backup: ${dbFile.getName} -> ${zipBackup.getName}")
        log.info(s"To restore the database, run 'db-restore ${zipBackup.getName}'.")
        Script.main(
          "-url", s"jdbc:h2:$dbPathWithoutExtension", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
        )
        log.info(s"Sql script database backup created.")
      finally
        cache.MemCache.availableMem.addAndGet(64000000)
        awaitable.countDown()
    }, "db-backup").start()
    awaitable

  def restorePlainDbBackup(dbDir: File): Unit =
    val database = dbFile(dbDir)
    val plainBackup = backupFile(dbDir)
    ensure("utility.restore.notFound", plainBackup.exists(), s"Database backup file $plainBackup does not exist")
    log.info(s"Restoring plain database backup: ${plainBackup.getName} -> ${database.getName}")
    Files.copy(plainBackup.toPath, database.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

  def restoreSqlDbBackup(dbDir: File, scriptName: String): Unit =
    val script = File(dbDir, scriptName)
    ensure("utility.restore.from", script.exists(), s"Database backup script file $script does not exist")
    val database = dbFile(dbDir)
    ensure("utility.restore", !database.exists || database.delete, s"Can't delete current database file $database")
    log.info(s"Restoring database backup: ${script.getName} -> ${database.getName}")
    RunScript.main(
      "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
    )

  def compactDb(dbDir: File): Unit = withDb(dbDir, readOnly = false)(_.shutdownCompact())
  
  def stats(dbDir: File): Unit = withDb(dbDir, readOnly = true, checkVersion = false) { db =>
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

  def list(dbDir: File, path: String): Unit = withDb(dbDir, readOnly = true) { db =>
    db.entry(path) match
      case None =>
        println(s"The path '$path' does not exist.")
      case Some(file: FileEntry) =>
        println(s"File information for path '$path':")
        println(s"${file.name} .. ${readableBytes(db.logicalSize(file.dataId))}")
      case Some(dir: DirEntry) =>
        println(s"Listing of directory '$path':")
        db.children(dir.id).sortBy {
          case dir: DirEntry => s"d${dir.name}"
          case file: FileEntry => s"f${file.name}"
        }.foreach {
          case dir: DirEntry => println(s"> ${dir.name}")
          case file: FileEntry => println(s"- ${file.name} ${"." * math.max(2, 38-file.name.length)} ${readableBytes(db.logicalSize(file.dataId))}")
        }
  }

  def del(dbDir: File, path: String): Unit = withDb(dbDir, readOnly = false) { db =>
    db.entry(path) match
      case None =>
        println(s"The path '$path' does not exist.")
      case Some(file: FileEntry) =>
        if db.deleteChildless(file.id)
        then log.info(s"Marked deleted file '$path' .. ${readableBytes(db.logicalSize(file.dataId))}")
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

  def find(dbDir: File, nameLike: String): Unit = withDb(dbDir, readOnly = true) { db =>
    println(s"Searching for files matching '$nameLike':")

    @annotation.tailrec
    def path(entry: TreeEntry, acc: List[TreeEntry] = Nil): List[TreeEntry] =
      if entry.id == root.id then entry :: acc else
        db.entry(entry.parentId) match // fold can not be used tail recursively
          case None => Nil
          case Some(parent) => path(parent, entry :: acc)

    nameLike.split("/").filter(_.nonEmpty).lastOption match
      case None =>
        println("Matcher is empty, can't search.")
      case Some(last) =>
        val sqlMatcher = last.replaceAll("\\*", "%").replaceAll("\\?", "_")
        val pathMatcher = nameLike.replaceAll("\\.", "\\.").replaceAll("\\*", ".*").replaceAll("\\?", ".")
        db.entryLike(sqlMatcher).foreach { entry =>
          path(entry) match
            case Nil => /* deleted entry */
            case entries =>
              val fullPath = entries.map {
                case dirEntry: DirEntry => s"${dirEntry.name}/"
                case fileEntry: FileEntry => fileEntry.name
              }.mkString
              if fullPath.matches(s".*$pathMatcher.*") then println(fullPath)
        }
  }

  def reclaimSpace(dbDir: File, keepDeletedDays: Int): Unit = withDb(dbDir, readOnly = false) { db =>
    log.info(s"Reclaiming space from deleted files and orphan data entries now.")

    log.info(s"Deleting tree entries marked for deletion more than $keepDeletedDays days ago...")

    // First un-root, then delete. Deleting directly can violate the foreign key constraint.
    log.info(s"Part 1: Mark the tree entries to delete...")
    while
      log.info(s"Number of entries marked: ${db.unrootDeletedEntries(now.asLong - keepDeletedDays*24*60*60*1000)}")
      // The following takes care of a special case that is supposed never to happen.
      val fixedEntries = db.childrenOfDeletedUnrootedElements().tapEach(db.unrootAndMarkDeleted).size
      if fixedEntries > 0 then log.info(s"Fixed $fixedEntries incompletely deleted entries.")
      fixedEntries > 0
    do ()

    log.info(s"Part 2: Deleting marked tree entries...")
    log.info(s"Number of tree entries deleted: ${db.deleteUnrootedEntries()}")

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
    log.info("      the reclaim process will result in partial data corruption.")
  }
