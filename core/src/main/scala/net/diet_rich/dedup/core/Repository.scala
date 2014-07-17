// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.data.DataSettings
import net.diet_rich.dedup.core.sql.{CurrentDatabase, DatabaseSlice}
import net.diet_rich.dedup.core.values.{Size, StoreMethod, Hash}
import net.diet_rich.dedup.util.EnhancedFile

object Repository {
  def databaseDirectory(repositoryDirectory: File) = repositoryDirectory / "database"
  def datafilesDirectory(repositoryDirectory: File) = repositoryDirectory / "datafiles"
  def productionDatabase(repositoryDirectory: File, readonly: Boolean) =
    sql.ProductionDatabase fromFile (databaseDirectory(repositoryDirectory) / "dedup", readonly)

  def apply[T](
    repositoryDirectory: File,
    storeMethod: StoreMethod = StoreMethod.DEFLATE,
    readonly: Boolean = false,
    processingThreadPoolSize: Int = 8,
    storeThreadPoolSize: Int = 8,
    fileHandlesPerStoreThread: Int = 4
  )(application: FileSystem => T): T = {
    trait ConfigurationPart extends sql.ThreadSpecificSessionsPart with StoreSettingsSlice with data.DataSettingsSlice {
      override val database: CurrentDatabase = productionDatabase(repositoryDirectory, readonly)
      private val settingsFromDatabase = database withSession (sql.DBUtilities.allSettings(_))
      override val storeSettings = StoreSettings(settingsFromDatabase(hashAlgorithmKey), processingThreadPoolSize, storeMethod)
      override val dataSettings = data.DataSettings(
        Size(settingsFromDatabase(data.blocksizeKey).toLong),
        datafilesDirectory(repositoryDirectory),
        storeThreadPoolSize, fileHandlesPerStoreThread, readonly
      )
      require(settingsFromDatabase(sql.databaseVersionKey) == sql.databaseVersionValue, s"${sql.databaseVersionKey} in database has value ${settingsFromDatabase(sql.databaseVersionKey)} but expected ${sql.databaseVersionValue}")
      require(settingsFromDatabase(data.versionKey) == data.versionValue, s"${data.versionKey} in database has value ${settingsFromDatabase(data.versionValue)} but expected ${data.versionValue}")
    }
    val fileSystem = new FileSystem with ConfigurationPart with FileSystem.BasicPart with data.DataStorePart

    fileSystem.inLifeCycle(application(fileSystem))
  }

  def create(
    repositoryDirectory: File,
    hashAlgorithm: String = "MD5",
    dataBlockSize: Size = Size(0x4000000) // 64MB
  ) = {
    require(repositoryDirectory.isDirectory, s"$repositoryDirectory is not a directory")
    require(repositoryDirectory.list.size == 0, s"$repositoryDirectory is not empty")
    require(databaseDirectory(repositoryDirectory) mkdir(), s"could not create database directory in $repositoryDirectory")

    val settingsToWrite = Map(
      hashAlgorithmKey -> hashAlgorithm,
      data.blocksizeKey -> dataBlockSize.value.toString,
      data.versionKey -> data.versionValue,
      sql.databaseVersionKey -> sql.databaseVersionValue
    )
    productionDatabase(repositoryDirectory, false) withSession { implicit session =>
      sql.DBUtilities.createTables(Hash digestLength hashAlgorithm)
      sql.DBUtilities.recreateIndexes
      sql.DBUtilities.replaceSettings(settingsToWrite)
    }
  }
}
