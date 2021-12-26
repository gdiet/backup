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
