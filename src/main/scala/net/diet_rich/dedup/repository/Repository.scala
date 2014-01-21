// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.util.io._
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.datastore.DataStore2

class Repository(val basedir: File, val readonly: Boolean, enableDbShutdownHook: Boolean = true) { import Repository._
  val settings = readSettingsFile(basedir.child(settingsFileName))
  
  val digesters = new HashDigester(settings(hashKey)) with Digesters with CrcAdler8192
  val dataStore = new DataStore2(basedir, settings(dataSizeKey).toInt, readonly)

  private val dbdir = basedir.child(dbDirName)
  private implicit val connection = getConnection(dbdir, readonly, enableDbShutdownHook)
  private val lockfile = dbdir.child(s"$dbFileName.lock.db")
  if (!readonly) require(lockfile.isFile, s"Expected database lock file $lockfile to exist.")
  
  val fs: BackupFileSystem = new BackupFileSystem(digesters, dataStore)

  if (!checkSettings(basedir)) {
    shutdown(false)
    throw new IllegalStateException("Repository or database settings are not OK")
  }

  def shutdown(backupDb: Boolean) = {
    dataStore.shutdown
    execUpdate("SHUTDOWN")
    Thread.sleep(300)
    if (!readonly) require(!lockfile.exists, s"Expected database lock file $lockfile not to exist any more.")
    
    if (backupDb) {
      val dateString = new java.text.SimpleDateFormat("yy-MM-dd_HH-mm-ss").format(new java.util.Date)
      import java.util.zip._
      import java.io._
      val zipOut = new ZipOutputStream(new FileOutputStream(basedir.child(s"DBbackup_$dateString.zip")))
      zipOut.setLevel(9)
      def addEntry(path: String): Unit = {
        val file = basedir.child(path)
        require(file.exists())
        if (file.isDirectory()) {
          file.list().foreach(child => addEntry(path+"/"+child))
        } else {
          require(file.isFile())
          zipOut.putNextEntry(new ZipEntry(path))
          using(new FileInputStream(file)) { _.copyTo(zipOut) }
          zipOut.closeEntry()
        }
      }
      addEntry(settingsFileName)
      addEntry(dbDirName)
      zipOut.close
    }
  }
}

object Repository {
  val dbDirName = "database"
  val dbFileName = "dedup"
  val settingsFileName = "settings.txt"
    
  // initial repository version is 1.0
  // new in repository version 1.1: changed data file header
  val repositoryVersion = "1.1"
  // initial database version is 1.0
  // new in database version 1.1: CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted)
  val dbVersion = "1.1"
    
  val repositoryVersionKey = "repository version"
  val repositoryIdKey = "repository id"
  val hashKey = "hash algorithm"
  val dataSizeKey = "data size"
  val dbVersionKey = "database version"
    
  def getConnection(basedir: File, readonly: Boolean, enableShutdownHook: Boolean = true) =
    DBConnection.forH2(s"$basedir/$dbFileName", readonly, enableShutdownHook)
    
  def readFileSettings(basedir: File): Map[String, String] =
    readSettingsFile(basedir.child(Repository.settingsFileName))

  def writeFileSettings(basedir: File, repositorySettings: Map[String, String]) =
    writeSettingsFile(basedir.child(Repository.settingsFileName), repositorySettings)
    
  def checkSettings(basedir: File)(implicit connection: java.sql.Connection): Boolean = {
    val fileSettings = readFileSettings(basedir)
    val dbSettings = SettingsDB.readDbSettings
    fileSettings == dbSettings &&
    dbSettings(repositoryVersionKey) == repositoryVersion &&
    dbSettings(dbVersionKey) == dbVersion
  }
}
