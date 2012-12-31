// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

class Repository(val basedir: java.io.File) { import Repository._
  val connection = DBConnection.forH2("%s/%s/%s" format (basedir, dbDirName, dbFileName))
}
object Repository {
  val dbDirName = "h2db"
  val dbFileName = "dedup"
  val settingsFileName = "settings.txt"
    
  val repositoryVersion = "1.0"
    
  val repositoryVersionKey = "repository version"
  val hashKey = "hash algorithm"
}