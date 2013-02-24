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
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Creates a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to create",
    HASH -> "MD5" -> "[%s <algorithm>] Hash algorithm to use, default '%s'",
    DATASIZE -> "8000000" -> "[%s <size>] Data size of the data files, default '%s'"
  )
  
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    val repositoryFolder = new File(opts(REPOSITORY))
    con.println(s"creating repository in $repositoryFolder ...")
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
    implicit val connection = Repository.getConnection(dbdir, false)
    TreeDB.createTable
    DataInfoDB.createTable(Hash(Hashes.zeroBytesHash(hashAlgorithm)), CrcAdler8192.zeroBytesPrint)
    ByteStoreDB.createTable
    SettingsDB.createTable(repositorySettings)
    connection.con.close()
    con.println(s"... Success!")
  }
}
