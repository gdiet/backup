package dedup
package db

import org.h2.tools.{RunScript, Script}

import java.io.{File, FileInputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.sql.{Connection, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.SortedMap
import scala.util.Using.resource
import H2.{dbFileName, dbName}

def withConnection(dbDir: File, readonly: Boolean = true)(f: Connection => Any): Unit =
  resource(H2.connection(dbDir, readonly, dbMustExist = true))(f)
def withStatement(dbDir: File, readonly: Boolean = true)(f: Statement => Any): Unit =
  withConnection(dbDir, readonly)(con => resource(con.createStatement())(f))

object maintenance extends util.ClassLogging:

  def backup(dbDir: File, fileNameSuffix: String = ""): Unit =
    val dbFile = File(dbDir, dbFileName)
    ensure("tool.backup", dbFile.exists(), s"Database file $dbFile does not exist")
    val plainBackup = File(dbDir, s"$dbFileName.backup")
    log.info(s"Creating plain database backup: $dbFile -> $plainBackup")
    Files.copy(dbFile.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

    val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
    val zipBackup = File(dbDir, s"dedupfs_$dateString$fileNameSuffix.zip")
    log.info(s"Creating sql script database backup: $dbFile -> $zipBackup")
    Script.main(
      "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
    )

  def restoreBackup(dbDir: File, from: Option[String]): Unit = from match
    case None =>
      val dbFile = File(dbDir, dbFileName)
      val backup = File(dbDir, s"$dbFileName.backup")
      ensure("tool.restore.notfound", backup.exists(), s"Database backup file $backup does not exist")
      log.info(s"Restoring plain database backup: $backup -> $dbFile")
      Files.copy(backup.toPath, dbFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

    case Some(scriptName) =>
      val script = File(dbDir, scriptName)
      ensure("tool.restore.from", script.exists(), s"Database backup script file $script does not exist")
      val dbFile = File(dbDir, dbFileName)
      ensure("tool.restore", !dbFile.exists || dbFile.delete, s"Can't delete current database file $dbFile")
      RunScript.main(
        "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
      )

  def stats(dbDir: File): Unit = withStatement(dbDir) { stat =>
    import Database.*
    log.info(s"Dedup File System Statistics")
    dbVersion(stat) match
      case None => log.error("No database version available.")
      case Some(`currentDbVersion`) => log.info(s"Database version $currentDbVersion - OK.")
      case Some(otherDbVersion) => log.warn(s"Database version $otherDbVersion is INCOMPATIBLE, expected $currentDbVersion.")
    val storageSize = stat.query("SELECT MAX(stop) FROM DataEntries")(_.withNext(_.getLong(1)))
    log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d Bytes) / ${stat.query("SELECT COUNT(id) FROM DataEntries WHERE seq = 1")(_.withNext(_.getLong(1)))}%,d entries")
    log.info(f"Files: ${stat.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL")(_.withNext(_.getLong(1)))}%,d, deleted ${stat.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL")(_.withNext(_.getLong(1)))}%,d")
    log.info(f"Folders: ${stat.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL")(_.withNext(_.getLong(1)))}%,d, deleted ${stat.query("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL")(_.withNext(_.getLong(1)))}%,d")
  }

  def list(dbDir: File, path: String): Unit = withConnection(dbDir) { con =>
    val db = Database(con)
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

  def del(dbDir: File, path: String): Unit = withConnection(dbDir, readonly = false) { con =>
    val db = Database(con)
    db.entry(path) match
      case None =>
        println(s"The path '$path' does not exist.")
      case Some(file: FileEntry) =>
        db.delete(file.id)
        log.info(s"Marked deleted file '$path' .. ${readableBytes(db.dataSize(file.dataId))}")
      case Some(dir: DirEntry) =>
        log.info(s"Marking deleted directory '$path' ...")
        def delete(treeEntry: TreeEntry): Long =
          val childCount = db.children(treeEntry.id).map(delete).sum
          db.delete(treeEntry.id)
          log.debug(s"Marked deleted: $treeEntry")
          childCount + 1
        log.info(s"Marked deleted ${delete(dir)} files/directories.")
  }

  def find(dbDir: File, matcher: String): Unit = withConnection(dbDir) { con =>
    println(s"Searching for files matching '$matcher':")

    val qLike = con.prepareStatement(
      """SELECT id, parentId, name, time, dataId FROM TreeEntries
        |WHERE deleted = 0 AND name LIKE ?""".stripMargin
    )
    val qId = con.prepareStatement(
      """SELECT id, parentId, name, time, dataId FROM TreeEntries
        |WHERE id = ? AND deleted = 0""".stripMargin
    )
    def treeEntry(rs: ResultSet): TreeEntry = rs.opt(_.getLong(5)) match
      case None         => DirEntry (rs.getLong(1), rs.getLong(2), rs.getString(3), Time(rs.getLong(4))                )
      case Some(dataId) => FileEntry(rs.getLong(1), rs.getLong(2), rs.getString(3), Time(rs.getLong(4)), DataId(dataId))

    @annotation.tailrec
    def path(entry: TreeEntry, acc: List[TreeEntry] = Nil): List[TreeEntry] =
      if entry.id == root.id then entry :: acc else
        qId.setLong(1, entry.parentId)
        qId.query(_.maybeNext(treeEntry)) match
          case None => Nil
          case Some(parent) => path(parent, entry :: acc)

    qLike.setString(1, matcher)
    qLike.query(_.seq(treeEntry)).foreach { entry =>
      path(entry) match
        case Nil => /* deleted entry */
        case entries =>
          println(entries.map {
            case dirEntry: DirEntry => s"${dirEntry.name}/"
            case fileEntry: FileEntry => fileEntry.name
          }.mkString)
    }
  }

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

    // TODO extract to method and add integration test
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

    // TODO add option to skip this
    log.info(s"Checking compaction potential of the data storage:")
    Database.freeAreas(stat) // Run for its log output

    log.info(s"Compacting database...")
    stat.execute("SHUTDOWN COMPACT;")
    log.info(s"Finished stage 1 of reclaiming space. Undo by restoring the database from a backup.")
  }

object blacklist extends util.ClassLogging:

  /** @param dbDir Database directory
    * @param blacklistDir Directory containing files to add to the blacklist
    * @param deleteFiles If true, files in the `blacklistDir` are deleted when they have been taken over
    * @param dfsBlacklist Name of the base blacklist folder in the dedup file system, resolved against root
    * @param deleteCopies If true, mark deleted all blacklisted occurrences except for the original entries in `dfsBlacklist` */
  def apply(dbDir: File, blacklistDir: String, deleteFiles: Boolean, dfsBlacklist: String, deleteCopies: Boolean): Unit = withConnection(dbDir, readonly = false) { connection =>
    val db = Database(connection)
    db.mkDir(root.id, dfsBlacklist).foreach(_ => log.info(s"Created blacklist folder DedupFS:/$dfsBlacklist"))
    db.child(root.id, dfsBlacklist) match
      case None                          => log.error(s"Can't run blacklisting - couldn't create DedupFS:/$dfsBlacklist.")
      case Some(_: FileEntry)            => log.error(s"Can't run blacklisting - DedupFS:/$dfsBlacklist is a file, not a directory.")
      case Some(blacklistRoot: DirEntry) => resource(connection.createStatement()) { stat =>
        log.info(s"Blacklisting now...")

        // Add external files to blacklist.
        val blacklistFolder = File(blacklistDir).getCanonicalFile
        val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
        db.mkDir(blacklistRoot.id, dateString).foreach(externalFilesToInternalBlacklist(db, blacklistFolder, _, deleteFiles))

        // Process internal blacklist.
        processInternalBlacklist(db, connection, stat, dfsBlacklist, s"/${blacklistRoot.name}", blacklistRoot.id, deleteFiles)
        log.info("Compacting database...")
        stat.execute("SHUTDOWN COMPACT;")
        log.info(s"Finished blacklisting.")
      }
  }

  // TODO integration test
  def externalFilesToInternalBlacklist(db: Database, currentDir: File, dirId: Long, deleteFiles: Boolean): Unit =
    Option(currentDir.listFiles()).toSeq.flatten.foreach { file =>
      if file.isDirectory then
        db.mkDir(dirId, file.getName).foreach(externalFilesToInternalBlacklist(db, file, _, deleteFiles))
        if !deleteFiles then {} else // needed like this to avoid compile problem
          if file.listFiles.isEmpty then file.delete else
            log.warn(s"Blacklist folder not empty after processing it: $file")
      else
        val (size, hash) = resource(FileInputStream(file)) { stream =>
          val buffer = new Array[Byte](memChunk)
          val md = java.security.MessageDigest.getInstance(hashAlgorithm)
          val size = Iterator.continually(stream.read(buffer)).takeWhile(_ > 0)
            .tapEach(md.update(buffer, 0, _)).map(_.toLong).sum
          size -> md.digest()
        }
        val dataId = db.dataEntry(hash, size).getOrElse(
          DataId(db.nextId).tap(db.insertDataEntry(_, 1, size, 0, 0, hash))
        )
        db.mkFile(dirId, file.getName, Time(file.lastModified), dataId)
        if deleteFiles && file.delete then
          log.info(s"Moved to DedupFS blacklist: $file")
        else
          log.info(s"Copied to DedupFS blacklist: $file")
    }

  // TODO integration test
  def processInternalBlacklist(db: Database, connection: Connection, stat: Statement, dfsBlacklist: String, parentPath: String, parentId: Long, deleteCopies: Boolean): Unit =
    db.children(parentId).foreach {
      case dir: DirEntry =>
        processInternalBlacklist(db, connection, stat, dfsBlacklist, s"$parentPath/${dir.name}", dir.id, deleteCopies)
      case file: FileEntry =>
        val size = stat.query(
          s"SELECT stop - start FROM DataEntries WHERE ID = ${file.dataId}"
        )(_.seq(_.getLong(1))).sum
        if size > 0 then
          log.info(s"Blacklisting $parentPath/${file.name}")
          connection.transaction {
            stat.executeUpdate(s"DELETE FROM DataEntries WHERE id = ${file.dataId} AND seq > 1")
            stat.executeUpdate(s"UPDATE DataEntries SET start = 0, stop = 0 WHERE id = ${file.dataId}")
          }
        if deleteCopies then
          @annotation.tailrec
          def pathOf(id: Long, pathEnd: String): String =
            val parentId -> name = stat.query(
              s"SELECT parentId, name FROM TreeEntries WHERE id = $id"
            )(_.one(r => (r.getLong(1), r.getString(2))))
            val path = s"/$name$pathEnd"
            if parentId == 0 then path else pathOf(parentId, path)
          val copies = stat.query(
            s"SELECT id, parentId, name FROM TreeEntries WHERE dataId = ${file.dataId} AND deleted = 0 AND id != ${file.id}"
          )(_.seq(r => (r.getLong(1), r.getLong(2), r.getString(3))))
          val filteredCopies = copies
            .map((id, parentId, name) => (id, pathOf(parentId, s"/$name")))
            .filterNot(_._2.startsWith(s"/$dfsBlacklist/"))
          filteredCopies.foreach { (id, path) =>
            log.info(s"Deleting copy of entry: $path")
            db.delete(id)
          }
    }
