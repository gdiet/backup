// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.data.DataSettings
import net.diet_rich.dedup.core.sql.{CurrentDatabase, DatabaseSlice}
import net.diet_rich.dedup.core.values.{Size, StoreMethod, Hash}
import net.diet_rich.dedup.util.io.{EnhancedFile, readSettingsFile, writeSettingsFile}

object Repository {
  private def repositoryContents(repositoryDirectory: File) = new {
    val databaseDirectory = repositoryDirectory / "database"
    val datafilesDirectory = repositoryDirectory / "datafiles"
    val settingsFile = repositoryDirectory / "settings.txt"
    def productionDatabase(readonly: Boolean) =
      sql.ProductionDatabase fromFile (databaseDirectory / "dedup", readonly)
  }

  def fileSystem(
    repositoryDirectory: File,
    storeMethod: StoreMethod = StoreMethod.DEFLATE,
    readonly: Boolean = false,
    processingThreadPoolSize: Int = 8,
    storeThreadPoolSize: Int = 8,
    fileHandlesPerStoreThread: Int = 4
  ): FileSystem = {
    val repo = repositoryContents(repositoryDirectory); import repo._
    val settingsFromFile = readSettingsFile(settingsFile)
    trait ConfigurationPart extends sql.ThreadSpecificSessionsPart with StoreSettingsSlice with data.DataSettingsSlice {
      override val database: CurrentDatabase = productionDatabase(readonly)
      private val settingsFromDatabase = database withSession (sql.DBUtilities.allSettings(_))
      override val storeSettings = StoreSettings(settingsFromDatabase(hashAlgorithmKey), processingThreadPoolSize, storeMethod)
      override val dataSettings = data.DataSettings(
        Size(settingsFromDatabase(data.blocksizeKey).toLong),
        datafilesDirectory,
        storeThreadPoolSize, fileHandlesPerStoreThread, readonly
      )
      require(settingsFromDatabase(sql.databaseVersionKey) == sql.databaseVersionValue, s"${sql.databaseVersionKey} in database has value ${settingsFromDatabase(sql.databaseVersionKey)} but expected ${sql.databaseVersionValue}")
      require(settingsFromDatabase(data.versionKey) == data.versionValue, s"${data.versionKey} in database has value ${settingsFromDatabase(data.versionValue)} but expected ${data.versionValue}")
      // TODO blocksize may differ, no problem in that. Separate settings file only for the data files settings?
      require(settingsFromDatabase == settingsFromFile, s"settings from file $settingsFile do not match settings from database.\nsettings from file: $settingsFromFile\nsettings from database: $settingsFromDatabase")
    }
    new FileSystem with ConfigurationPart with FileSystem.BasicPart
  }

  def apply[T](
    repositoryDirectory: File,
    storeMethod: StoreMethod = StoreMethod.DEFLATE,
    readonly: Boolean = false,
    processingThreadPoolSize: Int = 8,
    storeThreadPoolSize: Int = 8,
    fileHandlesPerStoreThread: Int = 4
  )(application: FileSystem => T): T = {
    val fs = fileSystem(repositoryDirectory, storeMethod, readonly, processingThreadPoolSize, storeThreadPoolSize, fileHandlesPerStoreThread)
    fs.inLifeCycle(application(fs))
  }

  def create(
    repositoryDirectory: File,
    hashAlgorithm: String = "MD5",
    dataBlockSize: Size = Size(0x4000000) // 64MB
  ) = {
    val settingsToWrite = Map(
      hashAlgorithmKey -> (Hash algorithmChecked hashAlgorithm),
      data.blocksizeKey -> dataBlockSize.value.toString,
      data.versionKey -> data.versionValue,
      sql.databaseVersionKey -> sql.databaseVersionValue
    )

    val repo = repositoryContents(repositoryDirectory); import repo._
    require(repositoryDirectory.isDirectory, s"$repositoryDirectory is not a directory")
    require(repositoryDirectory.list.size == 0, s"$repositoryDirectory is not empty")
    require(databaseDirectory mkdir(), s"could not create database directory in $repositoryDirectory")

    writeSettingsFile(settingsFile, settingsToWrite)
    productionDatabase(readonly = false) withSession { implicit session =>
      sql.DBUtilities.createTables(Hash digestLength hashAlgorithm)
      sql.DBUtilities.recreateIndexes
      sql.DBUtilities.replaceSettings(settingsToWrite)
    }
  }
}
