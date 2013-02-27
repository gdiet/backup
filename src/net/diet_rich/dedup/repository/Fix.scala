// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.dedup.database._

object Fix extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Repairs a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to repair",
    OPERATION -> "help" -> "[%s <operation>] Repair operation to execute or 'help' to list available repairs, default '%s'"
  )
  
  protected val dataFileHeadersOp = "recreateDataFileHeaders"
  protected val updateDatabaseOp = "updateDatabase"
   
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    opts(OPERATION) match {
      
      case "help" =>
        con.println("Available repairs:")
        con.println(s"$dataFileHeadersOp - recreate the headers of all data files")
        con.println(s"$updateDatabaseOp - update the database if necessary")

      case `updateDatabaseOp` =>
        val repositoryFolder = new File(opts(REPOSITORY))
        val dbdir = repositoryFolder.child(Repository.dbDirName)
        implicit val connection = Repository.getConnection(dbdir, false)
        val dbSettings = SettingsDB.readDbSettings
        val versionInDB = dbSettings(Repository.dbVersionKey)
        con.println(s"Current database version: ${Repository.dbVersion}")
        con.println(s"Version of database in ${opts(REPOSITORY)}: $versionInDB")
        versionInDB match {
          case Repository.dbVersion => con.println("Database is up to date.")
          
          case "1.0" =>
            net.diet_rich.util.sql.execUpdate("CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted)")
            SettingsDB.writeDbSettings(dbSettings + (Repository.dbVersionKey -> "1.1"))
            con.println("Updated database to version 1.1")
            
          case v =>
            con.println("ERROR: Don't know how to update a database version $v")
        }
        
      case `dataFileHeadersOp` =>
        val repository = new Repository(new java.io.File(opts(REPOSITORY)), false)
        try {
          val ds = repository.dataStore
          val dataFileNumber = 0L
          ds.recreateDataFileHeader(dataFileNumber)
          @annotation.tailrec
          def recreate(dataFileNumber: Long): Long =
            if (ds.recreateDataFileHeader(dataFileNumber)) {
              con.printProgress(s"Processing data file $dataFileNumber")
              recreate(dataFileNumber + 1)
            } else dataFileNumber
          val dataFilesProcessed = recreate(0)
          con.println(s"Recreated headers of $dataFilesProcessed data files.")
          con.println("Cleaning up ...")
        } finally { repository.shutdown(false) }
        con.println("Finished.")
        
      case op =>
        con.println(s"'$op' is not a supported repair operation.")
    }
  }
}
