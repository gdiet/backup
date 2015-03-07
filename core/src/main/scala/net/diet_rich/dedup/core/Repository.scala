package net.diet_rich.dedup.core

import java.io.{File, IOException}

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.core.meta.sql.{DBUtilities, SQLMetaBackendManager}
import net.diet_rich.dedup.util.{Memory, now, systemCores}
import net.diet_rich.dedup.util.io.RichFile

object Repository {
  val PRINTSIZE = 8192

  def create(root: File, repositoryid: Option[String] = None, hashAlgorithm: Option[String] = None, storeBlockSize: Option[Int] = None): Unit = {
    val actualRepositoryid = repositoryid getOrElse s"${util.Random.nextLong()}"
    require(root isDirectory, s"Root $root must be a directory")
    require(root.listFiles() isEmpty, s"Root $root must be empty")
    SQLMetaBackendManager create (root / metaDir, actualRepositoryid, hashAlgorithm getOrElse "MD5")
    FileBackend create (root / dataDir, actualRepositoryid, storeBlockSize getOrElse 64000000)
  }

  def readWrite(root: File, storeMethod: Option[Int] = None, parallel: Option[Int] = None): Repository = {
    val (metaBackend, initialFreeRanges) = SQLMetaBackendManager openInstance(root / metaDir, false)
    val fileBackend = new FileBackend(root / dataDir, metaBackend settings repositoryidKey, false)
    val freeRanges = new FreeRanges(initialFreeRanges, FileBackend.nextBlockStart(_, fileBackend.blocksize))
    new Repository(
      metaBackend,
      fileBackend,
      freeRanges,
      metaBackend settings metaHashAlgorithmKey,
      storeMethod getOrElse StoreMethod.STORE,
      parallel getOrElse systemCores
    )
  }

  def readOnly(root: File): RepositoryReadOnly = {
    val (metaBackend, _) = SQLMetaBackendManager openInstance(root / metaDir, true)
    val fileBackend = new FileBackend(root / dataDir, metaBackend settings repositoryidKey, true)
    new RepositoryReadOnly(metaBackend, fileBackend)
  }
}

class RepositoryReadOnly(val metaBackend: MetaBackend, dataBackend: DataBackend) extends AutoCloseable {
  private val memoryToReserve = 30000000
  require(Memory.reserve(memoryToReserve).isInstanceOf[Memory.Reserved], s"Could not reserve ${memoryToReserve/1000000} MB RAM for repository")

  override def close(): Unit = {
    suppressExceptions(metaBackend close())
    suppressExceptions(dataBackend close())
    suppressExceptions(Memory.free(memoryToReserve))
  }

  def read(dataid: Long): Iterator[Bytes] =
    metaBackend.dataEntry(dataid) match {
      case None => throw new IOException(s"No data entry found for id $dataid")
      case Some(entry) => StoreMethod.restoreCoder(entry.method)(readRaw(dataid))
    }

  private def readRaw(dataid: Long): Iterator[Bytes] =
    metaBackend.storeEntries(dataid).iterator
      .flatMap (dataBackend read)
}

class Repository(metaBackend: MetaBackend, dataBackend: DataBackend, freeRanges: FreeRanges, hashAlgorithm: String, storeMethod: Int, parallel: Int) extends RepositoryReadOnly(metaBackend, dataBackend) {

  protected val storeLogic: StoreLogicBackend = new StoreLogic(metaBackend, dataBackend.write, freeRanges, hashAlgorithm, storeMethod, parallel)

  override def close(): Unit = { suppressExceptions(storeLogic close()); super.close() }

  def createUnchecked(parent: Long, name: String, source: Option[Source] = None, time: Option[Long] = Some(now)): TreeEntry =
    metaBackend.createUnchecked(parent, name, time, source map storeLogic.dataidFor)

  def create(parent: Long, name: String, source: Option[Source] = None, time: Option[Long] = Some(now)): TreeEntry = metaBackend.inTransaction {
    val created = metaBackend.create(parent, name, time)
    try {
      metaBackend.change(created.id, parent, name, time, source map storeLogic.dataidFor)
      .getOrElse(throw new IOException("failed to update created file with data entry"))
    } catch {
      case e: IOException =>
        if (metaBackend.markDeleted(created.id)) throw e
        else throw new IOException("failed to delete partially created file", e)
    }
  }

  def createOrReplace(parent: Long, name: String, source: Option[Source] = None, time: Option[Long] = Some(now)): TreeEntry =
    metaBackend.createOrReplace(parent, name, time, source map storeLogic.dataidFor)
}
