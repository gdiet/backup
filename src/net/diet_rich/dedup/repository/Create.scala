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
  val usageHeader = "Creates a dedup repository. "
  val paramData = Seq(
    REPOSITORY -> "." -> "[%s <directory>] Location of the repository to create, default '%s'",
    HASH -> "MD5" -> "[%s <algorithm>] Hash algorithm to use, default '%s'"
  )
  
  run(args){ opts =>
    val repositoryFolder = new File(opts("-r"))
    require(repositoryFolder.isDirectory(), "Repository folder %s must be an existing directory" format repositoryFolder)
    require(repositoryFolder.list.isEmpty, "Repository folder %s must be empty" format repositoryFolder)
    val hashAlgorithm = opts("-h")
    val repositorySettings = Map(
      Repository.repositoryVersionKey -> Repository.repositoryVersion,
      Repository.hashKey -> Hashes.checkAlgorithm(hashAlgorithm)
    )
    require(repositoryFolder.child(Repository.dbDirName).mkdir(), "Could not create database folder %s" format Repository.dbDirName)
    writeSettingsFile(repositoryFolder.child(Repository.settingsFileName), repositorySettings)
    
    implicit val connection = Repository.getConnection(repositoryFolder)
    TreeDB.createTable
    DataInfoDB.createTable(Hash(Hashes.instance(hashAlgorithm).digest()), Print(0)) // FIXME Print(0)
  }
}
