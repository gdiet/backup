package dedup
package db

import org.h2.tools.Script

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Using.resource

object maintenance extends util.ClassLogging {
  def backup(repo: File): Unit =
    val dir = dbDir(repo).getAbsoluteFile
    val file = File(dir, "dedupfs.mv.db")
    require(file.exists(), s"Database file $file doesn't exist")
    val plainBackup = File(dir, "dedupfs.mv.db.backup")
    log.info(s"Creating plain database backup: $file -> $plainBackup")
    Files.copy(file.toPath, plainBackup.toPath, StandardCopyOption.REPLACE_EXISTING)

    val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
    val zipBackup = File(dir, s"dedupfs_$dateString.zip")
    log.info(s"Creating sql script database backup: $file -> $zipBackup")
    Script.main(
      "-url", s"jdbc:h2:$dir/dedupfs", "-script", s"$zipBackup", "-user", "sa", "-options", "compression", "zip"
    )

  def stats(repo: File): Unit =
    resource(H2.connection(repo, readonly = true)) { connection =>
      resource(connection.createStatement()) { stat =>
        log.info(s"Dedup File System Statistics")
        val storageSize = stat.executeQuery("SELECT MAX(stop) FROM DataEntries").tap(_.next()).getLong(1)
        log.info(f"Data storage: ${readableBytes(storageSize)} ($storageSize%,d Bytes) / ${stat.executeQuery("SELECT COUNT(id) FROM DataEntries WHERE seq = 1").tap(_.next()).getLong(1)}%,d entries")
        log.info(f"Files: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NOT NULL").tap(_.next()).getLong(1)}%,d")
        log.info(f"Folders: ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted = 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d, deleted ${stat.executeQuery("SELECT COUNT(id) FROM TreeEntries WHERE deleted <> 0 AND dataId IS NULL").tap(_.next()).getLong(1)}%,d")
      }
    }
}

