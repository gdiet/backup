package net.diet_rich.dedup.core.meta.sql

import java.io.{FileOutputStream, File}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.util.control.NonFatal

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.data.Hash
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.util.Logging
import net.diet_rich.dedup.util.io._

object SQLMetaBackendManager extends Logging {
  def create(metaRoot: File, repositoryid: String, hashAlgorithm: String) = {
    try { Hash digestLength hashAlgorithm } catch { case NonFatal(e) => require(false, s"Hash for $hashAlgorithm can't be computed: $e")}
    require(metaRoot mkdir(), s"Can't create meta directory $metaRoot")
    val settings = Map(
      metaVersionKey        -> metaVersionValue,
      repositoryidKey       -> repositoryid,
      metaHashAlgorithmKey  -> hashAlgorithm
    )
    writeSettingsFile(metaRoot / metaSettingsFile, settings)
    using(SessionFactory productionDB (metaRoot, readonly = false)) { sessionFactory =>
      DBUtilities.createTables(hashAlgorithm)(sessionFactory.session)
      DBUtilities.recreateIndexes(sessionFactory.session)
      new SQLMetaBackend(sessionFactory) replaceSettings settings
    }
    val status = Map(metaTimestampKey -> dateStringNow, metaCommentKey -> "repository created")
    writeSettingsFile(metaRoot / metaStatusFile, status)
  }

  protected def backupFileName(timestamp: String, comment: String): String = {
    val commentForFileName = comment.replaceAll("\\W", "_").replaceAll("_+", "_")
    s"${timestamp}_$commentForFileName.zip"
  }

  protected def zipFilesInto(target: File, files: Seq[File]): Unit =
    // TODO: Eventually, use the nio zip file system instead?
    using(new ZipOutputStream(new FileOutputStream(target))) { zipOut =>
      zipOut setLevel 9
      files foreach { file =>
        zipOut putNextEntry new ZipEntry(file getName)
        Files copy (file toPath, zipOut)
        zipOut closeEntry()
      }
    }

  protected def backupDatabase(metaRoot: File, backupFileName: String): Unit = {
    val databaseFile = metaRoot / "dedup.h2.db"
    val statusFile = metaRoot / metaStatusFile
    val backupDir = metaRoot / metaBackupDir
    val backupFile = backupDir / backupFileName
    require(databaseFile exists, s"the database file $databaseFile does not exist")
    require(statusFile exists, s"the status file $statusFile does not exist")
    if (!backupDir.exists) require(backupDir.mkdir, s"can't create backup directory $backupDir")
    require(!backupFile.exists, s"backup file $backupFile already exists")
    zipFilesInto(backupFile, Seq(statusFile, databaseFile))
  }

  def openInstance(metaRoot: File, readonly: Boolean, versionComment: Option[String]): (MetaBackend, Ranges) = {
    val settings = readSettingsFile(metaRoot / metaSettingsFile)
    val sessionFactory = SessionFactory productionDB (metaRoot, readonly = false)
    val problemRanges = DBUtilities.problemDataAreaOverlaps(sessionFactory.session)
    val freeInData = if (problemRanges isEmpty) DBUtilities.freeRangesInDataArea(sessionFactory.session) else Nil
    val freeRanges = freeInData.toVector :+ DBUtilities.freeRangeAtEndOfDataArea(sessionFactory.session)
    if (problemRanges nonEmpty) log.warn (s"Found data area overlaps: $problemRanges")
    val metaBackend = new SQLMetaBackend(sessionFactory) {
      override def close(): Unit = {
        super.close()
        if (!readonly) {
          val status = Map(metaTimestampKey -> dateStringNow, metaCommentKey -> "no comment")
          writeSettingsFile(metaRoot / metaStatusFile, status)
        }
      }
    }
    val settingsFromDB = metaBackend.settings
    if (settings != settingsFromDB) {
      metaBackend close()
      // FIXME simplify (everywhere)
      require(requirement = false, s"The settings in the database ${metaBackend settings} did not match with the expected settings $settings")
    }
    if (!readonly) {
      val statusFile = metaRoot / metaStatusFile
      val statusSettings = readSettingsFile(statusFile)
      val fileNameOfBackup = backupFileName(statusSettings(metaTimestampKey), statusSettings(metaCommentKey))
      backupDatabase(metaRoot, fileNameOfBackup)
      statusFile setWritable true
      require(statusFile delete(), s"could not delete status file $statusFile")
    }
    (metaBackend, freeRanges)
  }
}
