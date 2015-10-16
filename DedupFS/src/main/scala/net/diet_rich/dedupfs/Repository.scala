package net.diet_rich.dedupfs

import java.io.File
import net.diet_rich.bytestore.file.FileBackend
import net.diet_rich.common._, io._
import net.diet_rich.dedupfs.metadata.sql.SQLBackend

import scala.util.Random

object Repository extends DirWithConfig {
  override val objectName = "dedup file system repository"
  override val version = "3.0"
  private val (metaDirKey, dataDirKey) = ("metadata directory", "data directory")
  private val (metaDriverKey, bytestoreDriverKey) = ("metadata driver", "byte store driver")

  val PRINTSIZE = 8192

  def create(directory: File, repositoryid: Option[String], hashAlgorithm: Option[String], storeBlockSize: Option[Long]): Unit = {
    val actualRepositoryid: String = repositoryid getOrElse s"${Random nextLong()}"
    val actualHashAlgorithm: String = hashAlgorithm getOrElse "MD5"
    val actualBlockSize: Long = storeBlockSize getOrElse 64000000
    val settings = Map(
      metaDirKey -> "meta",
      dataDirKey -> "data",
      metaDriverKey -> "SQLBackend",
      bytestoreDriverKey -> "FileBackend"
    )
    initialize(directory, actualRepositoryid, settings)
    SQLBackend initialize (directory / settings(metaDirKey), actualRepositoryid, actualHashAlgorithm)
    FileBackend initialize (directory / settings(dataDirKey), actualRepositoryid, actualBlockSize)
  }
}
