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
    val dataSettingsFile = datafilesDirectory / "settings.txt"
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
    trait ConfigurationPart extends sql.ThreadSpecificSessionsPart with StoreSettingsSlice with data.DataSettingsSlice {
      override val database = productionDatabase(readonly)
      override val storeSettings = {
        val databaseSettings = database withSession (sql.DBUtilities.allSettings(_))
        require(databaseSettings(sql.databaseVersionKey) == sql.databaseVersionValue, s"${sql.databaseVersionKey} in database has value ${databaseSettings(sql.databaseVersionKey)} but expected ${sql.databaseVersionValue}")
        StoreSettings(databaseSettings(hashAlgorithmKey), processingThreadPoolSize, storeMethod)
      }
      override val dataSettings = {
        val datafilesSettings = readSettingsFile(dataSettingsFile)
        require(datafilesSettings(data.versionKey) == data.versionValue, s"${data.versionKey} in database has value ${datafilesSettings(data.versionValue)} but expected ${data.versionValue}")
        data.DataSettings(Size(datafilesSettings(data.blocksizeKey) toLong), datafilesDirectory, storeThreadPoolSize, fileHandlesPerStoreThread, readonly)
      }
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
    val databaseSettingsToWrite = Map(
      hashAlgorithmKey -> (Hash algorithmChecked hashAlgorithm),
      sql.databaseVersionKey -> sql.databaseVersionValue
    )
    val dataSettingsToWrite = Map(
      data.blocksizeKey -> dataBlockSize.value.toString,
      data.versionKey -> data.versionValue
    )

    val repo = repositoryContents(repositoryDirectory); import repo._
    require(repositoryDirectory.isDirectory, s"$repositoryDirectory is not a directory")
    require(repositoryDirectory.list.size == 0, s"$repositoryDirectory is not empty")
    require(databaseDirectory mkdir(), s"could not create database directory $databaseDirectory")
    require(datafilesDirectory mkdir(), s"could not create data directory $datafilesDirectory")

    productionDatabase(readonly = false) withSession { implicit session =>
      sql.DBUtilities.createTables(Hash digestLength hashAlgorithm)
      sql.DBUtilities.recreateIndexes
      sql.DBUtilities.replaceSettings(databaseSettingsToWrite)
    }
    writeSettingsFile(dataSettingsFile, dataSettingsToWrite)
  }
}
