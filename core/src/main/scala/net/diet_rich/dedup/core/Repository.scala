package net.diet_rich.dedup.core

import java.io.{File, IOException}

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.core.meta.sql.SQLMetaBackendManager
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io.RichFile

object Repository {
  val PRINTSIZE = 8192

  def create(root: File, repositoryid: Option[String], hashAlgorithm: Option[String], storeBlockSize: Option[Int]): Unit = {
    val actualRepositoryid = repositoryid getOrElse s"${util.Random.nextLong()}"
    require(root isDirectory, s"Root $root must be a directory")
    require(root.listFiles() isEmpty, s"Root $root must be empty")
    SQLMetaBackendManager create (root / metaDir, actualRepositoryid, hashAlgorithm getOrElse "MD5")
    FileBackend create (root / dataDir, actualRepositoryid, storeBlockSize getOrElse 64000000)
  }

  def openReadWrite(root: File, storeMethod: Option[Int], parallel: Option[Int], versionComment: Option[String]): RepositoryReadWrite = {
    val (metaBackend, initialFreeRanges) = SQLMetaBackendManager openInstance(root / metaDir, readWrite, versionComment)
    val fileBackend = new FileBackend(root / dataDir, metaBackend settings repositoryidKey, readWrite)
    val freeRanges = new FreeRanges(initialFreeRanges, FileBackend.nextBlockStart(_, fileBackend.blocksize))
    new RepositoryReadWrite(
      metaBackend,
      fileBackend,
      freeRanges,
      metaBackend settings metaHashAlgorithmKey,
      storeMethod getOrElse StoreMethod.STORE,
      parallel getOrElse systemCores
    )
  }

  def openReadOnly(root: File): Repository = {
    val (metaBackend, _) = SQLMetaBackendManager openInstance(root / metaDir, readOnly, None)
    val fileBackend = new FileBackend(root / dataDir, metaBackend settings repositoryidKey, readOnly)
    new Repository(metaBackend, fileBackend)
  }
}

class Repository(val metaBackend: MetaBackend, dataBackend: DataBackend) extends AutoCloseable with Logging {
  private val memoryToReserve = 30000000
  require(Memory.reserve(memoryToReserve).isInstanceOf[Memory.Reserved], s"Could not reserve ${memoryToReserve/1000000} MB RAM for repository")
  log.info("Dedup repository opened")

  override def close(): Unit = {
    log.warnOnException(metaBackend close())
    log.warnOnException(dataBackend close())
    log.warnOnException(Memory.free(memoryToReserve))
    log.info("Dedup repository closed")
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

class RepositoryReadWrite(metaBackend: MetaBackend, dataBackend: DataBackend, freeRanges: FreeRanges, hashAlgorithm: String, storeMethod: Int, parallel: Int) extends Repository(metaBackend, dataBackend) {

  val storeLogic: StoreLogicBackend = new StoreLogic(metaBackend, dataBackend.write, freeRanges, hashAlgorithm, storeMethod, parallel)

  override def close(): Unit = { log.warnOnException(storeLogic close()); super.close() }

  def createUnchecked(parent: Long, name: String, time: Long, source: Option[Source] = None): Long =
    metaBackend.createUnchecked(parent, name, Some(time), source map storeLogic.dataidFor)

  def create(parent: Long, name: String, source: Option[Source] = None, time: Option[Long] = someNow): Long = metaBackend.inTransaction {
    init(metaBackend.create(parent, name, time)) { id =>
      try {
        if (!metaBackend.change(id, parent, name, time, source map storeLogic.dataidFor))
          throw new IOException("failed to update created file with data entry")
      } catch {
        case e: IOException =>
          if (metaBackend.markDeleted(id)) throw e
          else throw new IOException("failed to delete partially created file", e)
      }
    }
  }

  def createOrReplace(parent: Long, name: String, source: Option[Source] = None, time: Option[Long] = someNow): Long =
    metaBackend.createOrReplace(parent, name, time, source map storeLogic.dataidFor)
}
