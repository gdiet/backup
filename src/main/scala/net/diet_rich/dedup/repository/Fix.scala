// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.util.sql._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.datastore.DataStore2

object Fix extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Repairs a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to repair",
    OPERATION -> "help" -> "[%s <operation>] Repair operation to execute or 'help' to list available repairs, default '%s'"
  )
  
  protected val updateRepositoryOp = "updateRepository"
  protected val updateDatabaseOp = "updateDatabase"
   
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    opts(OPERATION) match {
      
      case "help" =>
        con.println("Available repairs:")
        con.println(s"$updateRepositoryOp - update the repository if necessary")
        con.println(s"$updateDatabaseOp - update the database if necessary")

      case `updateDatabaseOp` =>
        implicit val (connection, repoSettings, dbSettings) = getConnectionAndSettings(opts)
        if (dbSettings != repoSettings) {
          con.println("ERROR: Settings in repository and in database do not match.")
          con.println("Exiting...")
        } else {
          val versionInDB = dbSettings(Repository.dbVersionKey)
          con.println(s"Current database version: ${Repository.dbVersion}")
          con.println(s"Version of database in ${opts(REPOSITORY)}: $versionInDB")
          versionInDB match {
            case Repository.dbVersion => con.println("Database is up to date.")
            
            case "1.0" =>
              con.println("Updating database from version 1.0 to version 1.1")
              con.println("Adding index idxTreeEntriesDeleted...")
              update("CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);")
              updateSettings(opts, (Repository.dbVersionKey -> "1.1"))
              con.println("Updated database to version 1.1")
              
            case "1.1" =>
              con.println("Updating database from version 1.0 to version 1.2")
              con.println("Adding sequence treeEntriesIdSeq...")
              update("CREATE SEQUENCE treeEntriesIdSeq START WITH SELECT MAX(id) + 1 FROM TreeEntries;")
              con.println("Using sequence treeEntriesIdSeq as default for TreeEntries id...")
              update("ALTER TABLE TreeEntries ALTER COLUMN id SET DEFAULT NEXT VALUE FOR treeEntriesIdSeq;")
              con.println("Setting parent = id for TreeEntries root entry...")
              update("UPDATE TreeEntries SET parent = id WHERE parent IS NULL;")
              con.println("Setting TreeEntries parent to NOT NULL...")
              update("ALTER TABLE TreeEntries ALTER COLUMN parent BIGINT NOT NULL;")
              con.println("Recreating TreeEntries indexes...")
              TreeDB.recreateIndexes
              con.println("Adding sequence dataEntriesIdSeq...")
              val maxIdInDataInfo  = query("SELECT MAX(id) FROM DataInfo;")(_ long 1).next
              val maxIdInByteStore = query("SELECT MAX(dataid) FROM ByteStore;")(_ long 1).next
              update(s"CREATE SEQUENCE dataEntriesIdSeq START ${math max (maxIdInDataInfo, maxIdInByteStore)};")
              con.println("Using sequence dataEntriesIdSeq as default for DataInfo id...")
              update("ALTER TABLE DataInfo ALTER COLUMN id SET DEFAULT NEXT VALUE FOR dataEntriesIdSeq;")
              updateSettings(opts, (Repository.dbVersionKey -> "1.2"))
              con.println("Updated database to version 1.2")
              
            case v =>
              con.println("ERROR: Don't know how to update a database version $v")
          }
        }
        connection.close
        
      case `updateRepositoryOp` =>
        val (connection, repoSettings, dbSettings) = getConnectionAndSettings(opts)
        if (dbSettings != repoSettings) {
          con.println("ERROR: Settings in repository and in database do not match.")
          con.println("Exiting...")
        } else {
          val versionInDB = dbSettings(Repository.repositoryVersionKey)
          con.println(s"Current repository version: ${Repository.repositoryVersion}")
          con.println(s"Version of repository in ${opts(REPOSITORY)}: $versionInDB")
          versionInDB match {
            case Repository.repositoryVersion => con.println("Repository is up to date.")
            
            case "1.0" =>
              con.println("Recreating data file headers...")
              
              val dataStore = new DataStore2(new File(opts(REPOSITORY)), dbSettings(Repository.dataSizeKey).toInt, false)
              try {
                var time = System.currentTimeMillis
                var timeDiff = 2000L
                @annotation.tailrec
                def recreate(dataFileNumber: Long): Long =
                  if (dataStore.recreateDataFileHeader(dataFileNumber)) {
                    if (time + timeDiff < System.currentTimeMillis()) {
                      con.printProgress(s"Processing data file $dataFileNumber")
                      time = System.currentTimeMillis()
                      timeDiff = timeDiff * 5 / 3
                    }
                    recreate(dataFileNumber + 1)
                  } else dataFileNumber
                val dataFilesProcessed = recreate(0)
                con.println(s"Recreated headers of $dataFilesProcessed data files.")
                con.println("Cleaning up ...")
              } finally { dataStore.shutdown }
              con.println("Finished.")
              
              updateSettings(opts, (Repository.repositoryVersionKey -> "1.1"))(connection)
              con.println("Updated repository to version 1.1")
          }
        }
        
      case op =>
        con.println(s"'$op' is not a supported repair operation.")
    }
  }

  def getConnectionAndSettings(opts: Map[String, String]) = {
    val repositoryFolder = new File(opts(REPOSITORY))
    val dbdir = repositoryFolder.child(Repository.dbDirName)
    implicit val connection = Repository.getConnection(dbdir, false)
    val dbSettings = SettingsDB.readDbSettings
    val repoSettings = Repository.readFileSettings(repositoryFolder)
    (connection, repoSettings, dbSettings)
  }

  def updateSettings(opts: Map[String, String], newSetting: (String, String))(implicit connection: java.sql.Connection) = {
    val dbSettings = SettingsDB.readDbSettings
    val newSettings = dbSettings + newSetting
    SettingsDB.writeDbSettings(newSettings)(connection)
    Repository.writeFileSettings(new File(opts(REPOSITORY)), newSettings)
  }
  
}
