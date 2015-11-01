package net.diet_rich.dedupfs

import java.io.File
import scala.util.Random

import net.diet_rich.bytestore.{ByteStore, ByteStoreRead}
import net.diet_rich.bytestore.file.FileBackend
import net.diet_rich.common._, io._
import net.diet_rich.dedupfs.metadata.{FreeRanges, MetadataRead, Metadata}
import net.diet_rich.dedupfs.metadata.sql.SQLBackend

object Repository extends DirWithConfigHelper {
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

  def openReadOnly(directory: File): ReadOnly =
    openAny(directory, { case (metaDir, storeDir, repositoryid) =>
      new RepositoryRead(SQLBackend.read(metaDir, repositoryid), FileBackend.read(storeDir, repositoryid))
    })

  def openReadWrite(directory: File, storeMethod: Int): Repository =
    openAny(directory, { case (metaDir, storeDir, repositoryid) =>
      val (metadata, freeRanges) = SQLBackend.readWrite(metaDir, repositoryid)
      val byteStore = FileBackend.readWrite(storeDir, repositoryid)
      new Repository(directory, metadata, byteStore, new FreeRanges(freeRanges, byteStore.nextBlockStart), storeMethod)
    })

  private def openAny[Repo <: Any](directory: File, repositoryFactory: (File, File, String) => Repo): Repo = {
    val settings = settingsChecked(directory, objectName)
    require(settings(bytestoreDriverKey) == "FileBackend", s"As $bytestoreDriverKey, only FileBackend is supported, not ${settings(bytestoreDriverKey)}")
    require(settings(metaDriverKey) == "SQLBackend", s"As $metaDriverKey, only SQLBackend is supported, not ${settings(metaDriverKey)}")
    repositoryFactory(directory / settings(metaDirKey), directory / settings(dataDirKey), settings(repositoryidKey))
  }

  type ReadOnly = RepositoryRead[MetadataRead, ByteStoreRead]
  type Any = RepositoryRead[_ <: MetadataRead, _ <: ByteStoreRead]
}

class RepositoryRead[Meta <: MetadataRead, Data <: ByteStoreRead](val metaBackend: Meta, val dataBackend: Data) extends AutoCloseable {
  override def close(): Unit = try metaBackend.close() finally dataBackend.close()
}

class Repository(val directory: File, metaBackend: Metadata, dataBackend: ByteStore, freeRanges: FreeRanges, storeMethod: Int) extends RepositoryRead(metaBackend, dataBackend) {
  val dirHelper = new DirWithConfig(Repository, directory)
  val storeLogic: StoreLogic = StoreLogic(metaBackend, dataBackend.write, freeRanges, metaBackend.hashAlgorithm, storeMethod, systemCores)
  dirHelper markOpen()
  override final def close(): Unit = { super.close(); dirHelper markClosed() }
}
