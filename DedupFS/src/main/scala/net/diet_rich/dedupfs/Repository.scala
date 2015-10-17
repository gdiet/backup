package net.diet_rich.dedupfs

import java.io.File
import net.diet_rich.bytestore.{ByteStore, ByteStoreRead}
import net.diet_rich.bytestore.file.FileBackend
import net.diet_rich.common._, io._
import net.diet_rich.dedupfs.metadata.{MetadataRead, Metadata}
import net.diet_rich.dedupfs.metadata.sql.SQLBackend

import scala.util.Random

object Repository extends DirWithConfig {
  override val objectName = "dedup file system repository"
  override val version = "3.0"
  private val (metaDirKey, dataDirKey) = ("metadata directory", "data directory")
  private val (metaDriverKey, bytestoreDriverKey) = ("metadata driver", "byte store driver")
  private val repositoryidKey = "repository id"

  val PRINTSIZE = 8192

  def create(directory: File, repositoryid: Option[String], hashAlgorithm: Option[String], storeBlockSize: Option[Long]): Unit = {
    val actualRepositoryid: String = repositoryid getOrElse s"${Random nextLong()}"
    val actualHashAlgorithm: String = hashAlgorithm getOrElse "MD5"
    val actualBlockSize: Long = storeBlockSize getOrElse 64000000
    val settings = Map(
      repositoryidKey -> actualRepositoryid,
      metaDirKey -> "meta",
      dataDirKey -> "data",
      metaDriverKey -> "SQLBackend",
      bytestoreDriverKey -> "FileBackend"
    )
    initialize(directory, objectName, settings)
    SQLBackend initialize (directory / settings(metaDirKey), actualRepositoryid, actualHashAlgorithm)
    FileBackend initialize (directory / settings(dataDirKey), actualRepositoryid, actualBlockSize)
  }

  def openReadOnly(directory: File): ReadOnly = {
    val settings = settingsChecked(directory, objectName)
    require(settings(bytestoreDriverKey) == "FileBackend", s"As $bytestoreDriverKey, only FileBackend is supported, not ${settings(bytestoreDriverKey)}")
    require(settings(metaDriverKey) == "SQLBackend", s"As $metaDriverKey, only SQLBackend is supported, not ${settings(metaDriverKey)}")
    val fileBackend = FileBackend.read(directory / settings(dataDirKey), settings(repositoryidKey))
    val metaBackend = SQLBackend.read(directory / settings(metaDirKey), settings(repositoryidKey))
    new RepositoryRead(metaBackend, fileBackend)
  }

  def openReadWrite(directory: File): Repository = {
    val settings = settingsChecked(directory, objectName)
    require(settings(bytestoreDriverKey) == "FileBackend", s"As $bytestoreDriverKey, only FileBackend is supported, not ${settings(bytestoreDriverKey)}")
    require(settings(metaDriverKey) == "SQLBackend", s"As $metaDriverKey, only SQLBackend is supported, not ${settings(metaDriverKey)}")
    val fileBackend = FileBackend.readWrite(directory / settings(dataDirKey), settings(repositoryidKey))
    val metaBackend = SQLBackend.readWrite(directory / settings(metaDirKey), settings(repositoryidKey))
    new Repository(metaBackend, fileBackend)
  }

  type ReadOnly = RepositoryRead[MetadataRead, ByteStoreRead]
  type Any = RepositoryRead[_ <: MetadataRead, _ <: ByteStoreRead]
}

class RepositoryRead[Meta <: MetadataRead, Data <: ByteStoreRead](val metaBackend: Meta, val dataBackend: Data) {
}

class Repository(metaBackend: Metadata, dataBackend: ByteStore) extends RepositoryRead(metaBackend, dataBackend) {
}
