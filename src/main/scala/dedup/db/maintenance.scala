package dedup
package db

import dedup.db.H2.{DBRef, dbRef, previousDbRef}
import org.h2.tools.{RunScript, Script}

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import scala.concurrent.{ExecutionContext, Future}

object maintenance extends util.ClassLogging:

  /** Create an SQL backup from the previous version's database file,
   *  then rename the file to "before_upgrade".
   *
   *  This method assumes that the right database driver for the previous
   *  version's database is in the classpath. */
  def migrateDbStep1(dbDir: File): Unit =
    main.info(s"Migrating old database to new version, step 1...")
    val currentDbFile = dbRef.dbFile(dbDir)
    ensure("maintenance.migrateDB.step1", ! currentDbFile.exists(),
      s"Can't migrate old database - a new database is already present: $currentDbFile")

    val previousDbFile = previousDbRef.dbFile(dbDir)
    ensure("maintenance.migrateDB.step1", previousDbFile.exists(),
      s"Can't migrate old database, not found: $previousDbFile")

    dbBackup(dbDir, previousDbRef, previousDbRef.beforeUpgrade).await

    ensure("maintenance.migrateDB.step1", previousDbFile.delete(),
      s"Could not delete $previousDbFile.")
    main.info(s"Migrating old database to new version, step 1 finished.")

  /** Restore the database from the file created in step 1,
   *  this time with the current database driver. */
  def migrateDbStep2(dbDir: File): Unit =
    main.info(s"Migrating old database to new version, step 2...")
    val currentDbFile = dbRef.dbFile(dbDir)
    ensure("maintenance.migrateDB.step1", ! currentDbFile.exists(),
      s"Can't migrate old database - a new database is already present: $currentDbFile")

    val backupFileNamePattern = previousDbRef.beforeUpgrade.dbZipNamePattern
    val backupCandidates = dbDir.listFiles().filter(_.getName.matches(backupFileNamePattern))
    ensure("maintenance.migrateDB.step2", backupCandidates.length == 1,
      s"Found no or too many DB backup candidates for migration step 2: ${backupCandidates.toList}")

    restoreSqlDbBackup(dbDir, backupCandidates(0).getName)
    main.info(s"Migrating old database to new version, step 2 finished.")

  /** @return A future that completes when the database backup is finished. */
  def dbBackup(dbDir: File, source: DBRef = dbRef, target: DBRef = dbRef.backup): Future[Unit] =
    plainDbBackup(dbDir, source, target)
    sqlDbBackup(dbDir, target, target)

  private def plainDbBackup(dbDir: File, source: DBRef, target: DBRef): Unit =
    val database = source.dbFile(dbDir)
    ensure("utility.backup", database.exists(), s"Database file $database does not exist")

    val plainBackup = target.dbFile(dbDir)
    log.info(s"Creating plain database backup: ${database.getName} -> ${plainBackup.getName}")

    Files.copy(database.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

  private def sqlDbBackup(dbDir: File, source: DBRef, target: DBRef): Future[Unit] =
    Future {
      try
        cache.MemCache.availableMem.addAndGet(-64000000) // Reserve some RAM for the backup process
        val zipBackup = target.dbZipFile(dbDir)
        log.info(s"Creating zipped SQL script database backup: ${source.dbFile(dbDir).getName} -> ${zipBackup.getName}")
        log.info(s"To restore the database, run 'db-restore ${zipBackup.getName}'.")
        Script.main(
          "-url", s"jdbc:h2:${source.dbScriptPath(dbDir)}", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
        )
        log.info(s"Zipped SQL script database backup created.")
      finally cache.MemCache.availableMem.addAndGet(64000000)
    }(ExecutionContext.global)

  def restorePlainDbBackup(dbDir: File): Unit =
    val backupFile = dbRef.backup.dbFile(dbDir)
    ensure("utility.restore.notFound", backupFile.exists(), s"Database backup file $backupFile does not exist")

    val database = dbRef.dbFile(dbDir)
    log.info(s"Restoring plain database backup: ${backupFile.getName} -> ${database.getName}")

    Files.copy(backupFile.toPath, database.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

  def restoreSqlDbBackup(dbDir: File, zipFileName: String): Unit =
    val zipFile = File(dbDir, zipFileName)
    ensure("utility.restore.from", zipFile.exists(), s"Database backup script zip file $zipFile does not exist")

    val database = dbRef.dbFile(dbDir)
    ensure("utility.restore", !database.exists || database.delete, s"Can't delete current database file $database")

    log.info(s"Restoring database backup: ${zipFile.getName} -> ${database.getName}")
    RunScript.main(
      "-url", s"jdbc:h2:${dbRef.dbScriptPath(dbDir)}", "-script", s"$zipFile", "-user", "sa", "-options", "compression", "zip"
    )

  def compactDb(dbDir: File): Unit =
    main.info(s"Compacting database...")
    withDb(dbDir, readOnly = false)(_.shutdownCompact())
  
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
    main.info(s"Marking deleted '$path' ...")
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
