// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.util.io._
import net.diet_rich.util.sql.WrappedConnection
import net.diet_rich.util.vals._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.datastore.DataStore

class Repository(val basedir: File) { import Repository._
  val settings = readSettingsFile(basedir.child(settingsFileName))
  val digesters = new HashDigester(settings(hashKey)) with Digesters with CrcAdler8192
  val dataStore = new DataStore(basedir, Size(settings(dataSizeKey).toLong))
  val fs: BackupFileSystem = new BackupFileSystem(digesters, dataStore)(getConnection(basedir))
}
object Repository {
  val dbDirName = "h2db"
  val dbFileName = "dedup"
  val settingsFileName = "settings.txt"
    
  val repositoryVersion = "1.0"
    
  val repositoryVersionKey = "repository version"
  val hashKey = "hash algorithm"
  val dataSizeKey = "data size"
    
  def getConnection(basedir: File): WrappedConnection = new Object {
    val con = DBConnection.forH2(s"$basedir/$dbDirName/$dbFileName")
  }
}