package dedup

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Date

object BackupDB {
  def run(options: Map[String, String]): Unit = {
    val repo = new File(options.getOrElse("repo", ".")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    val dbFile = new File(dbDir, "dedupfs.mv.db")
    if (!dbFile.exists()) throw new IllegalStateException(s"Database file $dbFile does not exist.")
    val timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm").format(new Date())
    val backup = new File(dbDir, s"dedupfs_$timestamp.mv.db")
    Files.copy(dbFile.toPath, backup.toPath, StandardCopyOption.COPY_ATTRIBUTES)
    println(s"Created database backup file $backup")
  }
}
