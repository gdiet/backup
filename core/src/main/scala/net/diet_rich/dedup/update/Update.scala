// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.update

import java.io.File

import scala.slick.jdbc.{SetParameter, GetResult, StaticQuery}

import net.diet_rich.dedup.core.{data, hashAlgorithmKey, Repository, zipBackup}
import net.diet_rich.dedup.core.sql.{DBUtilities, CurrentDatabase, ProductionDatabase}
import net.diet_rich.dedup.core.values.Hash
import net.diet_rich.dedup.util.ConsoleApp
import net.diet_rich.dedup.util.io.{writeSettingsFile, RichFile}

object LegacyDatabase {
  import scala.slick.driver.H2Driver.simple.Database

  def fromFile(file: File): CurrentDatabase =
    Database forURL (
      url = s"jdbc:h2:$file;ACCESS_MODE_DATA=r;MV_STORE=FALSE;MVCC=FALSE",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
    )
}

case class OldStoreEntry(dataid: Long, index: Int, start: Long, fin: Long)

object Update extends ConsoleApp {
  checkUsage("parameters: <repository path>")
  val repositoryDirectory = new File(repositoryPath)
  val repoContents = Repository repositoryContents repositoryDirectory; import repoContents._

  implicit val _getHash = GetResult(r => r nextBytes())
  implicit val _setHash = SetParameter((v: Array[Byte], p) => p setBytes v)
  implicit val _getStoreEntry = GetResult(r => OldStoreEntry(r <<, r <<, r <<, r <<))

  val selectOldSettings = StaticQuery.queryNA[(String, String)]("SELECT key, value FROM Settings;")
  val insertNewSetting = StaticQuery.update[(String, String)]("INSERT INTO Settings (key, value) VALUES (?, ?);")

  val selectOldTreeEntries = StaticQuery.queryNA[(Long, Option[Long], String, Int, Long, Option[Long], Option[Long])]("SELECT id, parent, name, type, time, deleted, dataid FROM TreeEntries;")
  val insertNewTreeEntry = StaticQuery.update[(Long, Long, String, Option[Long], Option[Long], Option[Long])]("INSERT INTO TreeEntries (id, parent, name, changed, dataid, deleted) VALUES (?, ?, ?, ?, ?, ?);")

  val selectOldDataEntries = StaticQuery.queryNA[(Long, Long, Long, Array[Byte], Int)]("SELECT id, length, print, hash, method FROM DataInfo;")
  val insertNewDataEntry = StaticQuery.update[(Long, Long, Long, Array[Byte], Int)]("INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")

  val selectOldStoreEntries = StaticQuery.queryNA[OldStoreEntry]("SELECT dataid, index, start, fin FROM ByteStore ORDER BY dataid, index;")
  val insertNewStoreEntry = StaticQuery.update[(Long, Long, Long)]("INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?);")

  val oldDb = LegacyDatabase fromFile (databaseDirectory / "dedup")
  val newDb = ProductionDatabase fromFile (databaseDirectory / "dedup-new", false)

  def error(message: String) = throw new IllegalArgumentException(message)

  oldDb withSession { oldSession =>
    val oldSettings = selectOldSettings.list(oldSession).toMap
    require(oldSettings("database version") == "1.1", "database version not 1.1")
    require(oldSettings("repository version") == "1.1", "repository version not 1.1")

    val hashAlgorithm = oldSettings("hash algorithm")
    val hashSize = Hash digestLength hashAlgorithm
    val repositoryID = oldSettings("repository id")
    val blockSize = oldSettings("data size")

    val newDatabaseSettings = Map(
      "database version" -> "2.0",
      hashAlgorithmKey -> hashAlgorithm,
      Repository.repositoryIdKey -> repositoryID
    )

    val newDataSettings = Map(
      data.blocksizeKey -> blockSize,
      data.versionKey -> data.versionValue,
      Repository.repositoryIdKey -> repositoryID
    )

    val oldStoreEntries = selectOldStoreEntries list oldSession groupBy (_.dataid)
    val reorderedStoreEntries = oldStoreEntries.map { case (_, list) =>
      list.foldLeft (List[OldStoreEntry]()) {
        case (Nil, item) => List(item)
        case (head :: tail, item) if head.fin == item.start => head.copy(fin = item.fin) :: tail
        case (newItems, item) => item :: newItems
      }.reverse
    }.flatten

    newDb withSession { newSession =>
      DBUtilities.createTables(hashSize)(newSession)
      DBUtilities.recreateIndexes(newSession)
      DBUtilities.replaceSettings(newDatabaseSettings)(newSession)

      reorderedStoreEntries foreach {
        case OldStoreEntry(dataid, _, start, fin) => insertNewStoreEntry(dataid, start, fin) execute newSession
      }

      selectOldTreeEntries.foreach {
        case     (0, None, "", 0, 0, None, None) => /* */
        case e @ (0, _, _, _, _, _, _) => error(s"unusual root entry: $e")
        case e @ (_, None, _, _, _, _, _) => error(s"entry without parent: $e")
        case (id, Some(parent), name, 0, modified, deleted, None) => insertNewTreeEntry(id, parent, name, Some(modified), None, deleted) execute newSession
        case (id, Some(parent), name, 1, modified, deleted, Some(dataid)) => insertNewTreeEntry(id, parent, name, Some(modified), Some(dataid), deleted) execute newSession
        case e => error(s"ignoring malformed tree node entry: $e")
      } (oldSession)
      selectOldDataEntries.foreach { x =>
        insertNewDataEntry(x) execute newSession
      } (oldSession)
    }

    (repositoryDirectory / "data") renameTo datafilesDirectory
    writeSettingsFile(dataSettingsFile, newDataSettings)
  }

  Thread sleep 1000 // wait for database shutdown and file lock release

  val oldDbFile = databaseDirectory / "dedup.h2.db"
  zipBackup(oldDbFile, repositoryDirectory, "DBbackup_%s.zip")
  oldDbFile.delete()

  val backupDirectory = repositoryDirectory / "database_version_1.1"
  backupDirectory.mkdir()
  repositoryDirectory.listFiles filter (_ isFile) foreach { file =>
    file.renameTo(backupDirectory / file.getName)
  }

  (databaseDirectory / "dedup-new.mv.db") renameTo (databaseDirectory / "dedup.mv.db")
  (repositoryDirectory / "data") renameTo (repositoryDirectory / "datafiles")
}
