// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.dedup.database._

object Check extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Checks a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to check",
    OPERATION -> "help" -> "[%s <operation>] Check to execute or 'help' to list available checks, default '%s'"
  )

  protected val showVersionsOp = "showVersions"
  protected val listDataDuplicatesOp = "listDataDuplicates"
  
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    opts(OPERATION) match {
      case "help" =>
        con.println("Available checks:")
        con.println(s"$listDataDuplicatesOp - list all duplicate data entries")
        con.println(s"$showVersionsOp - show versions and other meta data")
        
      case `listDataDuplicatesOp` =>
        ???

      case `showVersionsOp` =>
        val repositoryFolder = new File(opts(REPOSITORY))
        val dbdir = repositoryFolder.child(Repository.dbDirName)
        implicit val connection = Repository.getConnection(dbdir, false)
        val dbSettings = SettingsDB.readDbSettings
        val repoSettings = Repository.readFileSettings(repositoryFolder)
        con.println("This is a dedup system. System data:")
        con.println(s"${Repository.repositoryVersionKey}: ${Repository.repositoryVersion}")
        con.println(s"${Repository.dbVersionKey}: ${Repository.dbVersion}\n")
        if (dbSettings != repoSettings) {
          con.println("Settings in database and in repository do not match!\n")
          con.println("Settings in repository:")
          con.println(s"${repoSettings.toList.mkString("\n")}\n")
        }
        con.println("Settings in database:")
        con.println(s"${dbSettings.toList.mkString("\n")}\n")
        connection.close
        
      case op =>
        con.println(s"'$op' is not a supported check.")
    }
  }
}
