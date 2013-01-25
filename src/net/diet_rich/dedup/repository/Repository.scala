// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.util.io._
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.datastore.DataStore

class Repository(val basedir: File) { import Repository._
  val settings = readSettingsFile(basedir.child(settingsFileName))
  val digesters = new HashDigester(settings(hashKey)) with Digesters with CrcAdler8192
  val dataStore = new DataStore(basedir, Size(settings(dataSizeKey).toLong))

  private val dbdir = basedir.child(dbDirName)
  private implicit val connection = getConnection(dbdir)
  private val lockfile = dbdir.child(s"$dbFileName.lock.db")
  require(lockfile.isFile, s"Expected database lock file $lockfile to exist.")
  
  val fs: BackupFileSystem = new BackupFileSystem(digesters, dataStore)(connection)
  
  def shutdown(backupDb: Boolean) = {
    dataStore.shutdown
    execUpdate("SHUTDOWN")
    Thread.sleep(300)
    require(!lockfile.exists, s"Expected database lock file $lockfile not to exist any more.")
    
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
    
  val repositoryVersion = "1.0"
    
  val repositoryVersionKey = "repository version"
  val hashKey = "hash algorithm"
  val dataSizeKey = "data size"
    
  def getConnection(basedir: File): WrappedConnection = new Object {
    val con = DBConnection.forH2(s"$basedir/$dbFileName")
  }
}