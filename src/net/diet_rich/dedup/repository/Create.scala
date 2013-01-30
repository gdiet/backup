// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.dedup.database._

object Create extends CmdApp {
  def main(args: Array[String]): Unit =
    if (!run(args)(create)) System.exit(-1)
  
  val usageHeader = "Creates a dedup repository. "
  val paramData = Seq(
    REPOSITORY -> "." -> "[%s <directory>] Location of the repository to create, default '%s'",
    HASH -> "MD5" -> "[%s <algorithm>] Hash algorithm to use, default '%s'",
    DATASIZE -> "2000000" -> "[%s <size>] Data size of the data files, default '%s'"
  )
  
  def create(opts: Map[String, String]): Unit = {
    val repositoryFolder = new File(opts(REPOSITORY))
    val dataSize = opts(DATASIZE).toLong
    require(repositoryFolder.isDirectory(), s"Repository folder $repositoryFolder must be an existing directory")
    require(repositoryFolder.list.isEmpty, s"Repository folder $repositoryFolder must be empty")
    val hashAlgorithm = opts(HASH)
    val repositorySettings = Map(
      Repository.repositoryIdKey -> scala.util.Random.nextLong.toString,
      Repository.repositoryVersionKey -> Repository.repositoryVersion,
      Repository.hashKey -> Hashes.checkAlgorithm(hashAlgorithm),
      Repository.dataSizeKey -> dataSize.toString,
      Repository.dbVersionKey -> Repository.dbVersion
    )
    require(repositoryFolder.child(Repository.dbDirName).mkdir(), s"Could not create database folder ${Repository.dbDirName}")
    writeSettingsFile(repositoryFolder.child(Repository.settingsFileName), repositorySettings)
    
    val dbdir = repositoryFolder.child(Repository.dbDirName)
    implicit val connection = Repository.getConnection(dbdir)
    TreeDB.createTable
    DataInfoDB.createTable(Hash(Hashes.zeroBytesHash(hashAlgorithm)), CrcAdler8192.zeroBytesPrint)
    ByteStoreDB.createTable
    SettingsDB.createTable(repositorySettings)
  }
}
