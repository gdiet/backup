package dedup
package db

import org.h2.tools.{RunScript, Script}

import java.io.{File, FileInputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.SortedMap
import scala.util.Using.resource
import H2.{dbFileName, dbName}

object maintenance extends util.ClassLogging:

  def backup(dbDir: File): Unit =
    val dbFile = File(dbDir, dbFileName)
    require(dbFile.exists(), s"Database file $dbFile doesn't exist")
    val plainBackup = File(dbDir, s"$dbFileName.backup")
    log.info(s"Creating plain database backup: $dbFile -> $plainBackup")
    Files.copy(dbFile.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

    val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
    val zipBackup = File(dbDir, s"dedupfs_$dateString.zip")
    log.info(s"Creating sql script database backup: $dbFile -> $zipBackup")
    Script.main(
      "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
    )

  def restoreBackup(dbDir: File, from: Option[String]): Unit = from match
    case None =>
      val dbFile = File(dbDir, dbFileName)
      val backup = File(dbDir, s"$dbFileName.backup")
      require(backup.exists(), s"Database backup file $backup doesn't exist")
      log.info(s"Restoring plain database backup: $backup -> $dbFile")
      Files.copy(backup.toPath, dbFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)

    case Some(scriptName) =>
      val script = File(dbDir, scriptName)
      require(script.exists(), s"Database backup script file $script doesn't exist")
      val dbFile = File(dbDir, dbFileName)
      require(!dbFile.exists || dbFile.delete, s"Can't delete current database file $dbFile")
      RunScript.main(
        "-url", s"jdbc:h2:$dbDir/$dbName", "-script", s"$script", "-user", "sa", "-options", "compression", "zip"
      )

  def withConnection(dbDir: File, readonly: Boolean = true)(f: Connection => Any): Unit =
    resource(H2.connection(dbDir, readonly, dbMustExist = true))(f)
  def withStatement(dbDir: File, readonly: Boolean = true)(f: Statement => Any): Unit =
    withConnection(dbDir, readonly)(con => resource(con.createStatement())(f))

  def stats(dbDir: File): Unit = withStatement(dbDir) { stat =>
    log.info(s"Dedup File System Statistics")
    val storageSize = resource(stat.executeQuery("SELECT MAX(stop) FROM DataEntries"))(_.withNext(_.getLong(1)))
    log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d Bytes) / ${stat.executeQuery("SELECT COUNT(id) FROM DataEntries WHERE seq = 1").tap(_.next()).getLong(1)}%,d entries")
    log.info(f"Files: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d")
    log.info(f"Folders: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d")
  }

  /** @param dbDir Database directory
    * @param blacklistDir Directory containing files to add to the blacklist
    * @param deleteFiles If true, files in the `blacklistDir` are deleted when they have been taken over
    * @param dfsBlacklist Name of the base blacklist folder in the dedup file system, resolved against root
    * @param deleteCopies If true, mark deleted all blacklisted occurrences except for the original entries in `dfsBlacklist` */
  def blacklist(dbDir: File, blacklistDir: String, deleteFiles: Boolean, dfsBlacklist: String, deleteCopies: Boolean): Unit = withConnection(dbDir, readonly = false) { connection =>
    val db = Database(connection)
    db.mkDir(root.id, dfsBlacklist).foreach(_ => log.info(s"Created blacklist folder DedupFS:/$dfsBlacklist"))
    db.child(root.id, dfsBlacklist) match
      case None                          => log.error(s"Can't run blacklisting - couldn't create DedupFS:/$dfsBlacklist.")
      case Some(_: FileEntry)            => log.error(s"Can't run blacklisting - DedupFS:/$dfsBlacklist is a file, not a directory.")
      case Some(blacklistRoot: DirEntry) => resource(connection.createStatement()) { stat =>
        log.info(s"Blacklisting now...")

        // Add external files to blacklist.
        val blacklistFolder = File(blacklistDir).getCanonicalFile
        def recurseFiles(currentDir: File, dirId: Long): Unit =
          Option(currentDir.listFiles()).toSeq.flatten.foreach { file =>
            if file.isDirectory then
              db.mkDir(dirId, file.getName).foreach(recurseFiles(file, _))
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
        val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
        db.mkDir(blacklistRoot.id, dateString).foreach(recurseFiles(blacklistFolder, _))

        // Process internal blacklist.
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
                // If size is > 0, the log entry is already written.
                if size == 0 && copies.nonEmpty then log.info(s"Blacklisting $parentPath/${file.name}")
                copies.foreach { (id, parentId, name) =>
                  log.info(s"Deleting copy of entry: ${pathOf(parentId, "/")}$name")
                  db.delete(id)
                }
          }
        recurse(s"/$dfsBlacklist", blacklistRoot.id)
        log.info(s"Finished blacklisting.")
      }
  }
