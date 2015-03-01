package net.diet_rich.dedup.core

import java.io.{File, IOException}

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.core.meta.sql.{DBUtilities, SQLMetaBackendUtils}
import net.diet_rich.dedup.util.now
import net.diet_rich.dedup.util.io.RichFile

object Repository {
  val PRINTSIZE = 8192

  def create(root: File, repositoryID: Option[String] = None, hashAlgorithm: Option[String] = None, storeBlockSize: Option[Int] = None): Unit = {
    val actualRepositoryID = repositoryID getOrElse s"${util.Random.nextLong()}"
    require(root.isDirectory, s"Root $root must be a directory")
    require(root.listFiles().isEmpty, s"Root $root must be empty")
    SQLMetaBackendUtils.create(root / "meta", actualRepositoryID, hashAlgorithm getOrElse "MD5")
    FileBackend.create(root / "data", actualRepositoryID, storeBlockSize getOrElse 64000000)
  }

  def apply(root: File, readonly: Boolean, storeMethod: Option[Int] = None, storeThreads: Option[Int] = None): Repository = {
    SQLMetaBackendUtils.use(root / "meta", readonly) { case (metaBackend, metaSettings) =>
      val fileBackend = new FileBackend(root / "data", metaSettings(repositoryidKey), readonly)
      implicit val session = metaBackend.sessionFactory.session
      val problemRanges = DBUtilities.problemDataAreaOverlaps
      val freeInData = if (problemRanges isEmpty) DBUtilities.freeRangesInDataArea else Nil
      val rangesQueue = new RangesQueue(freeInData :+ DBUtilities.freeRangeAtEndOfDataArea, FileBackend.nextBlockStart(_, fileBackend.blocksize))
      new Repository(
        metaBackend,
        fileBackend,
        rangesQueue,
        metaSettings(metaHashAlgorithmKey),
        storeMethod getOrElse StoreMethod.STORE,
        storeThreads getOrElse 4
      )
    }
  }
}

class Repository(val metaBackend: MetaBackend, dataBackend: DataBackend, freeRanges: RangesQueue, hashAlgorithm: String, storeMethod: Int, storeThreads: Int) extends AutoCloseable {

  protected val storeLogic: StoreLogicBackend = new StoreLogic(metaBackend, dataBackend.write _, freeRanges, hashAlgorithm, storeMethod, storeThreads)

  override def close(): Unit = {
    storeLogic close()
    metaBackend close()
    dataBackend close()
  }
  
  def read(dataid: Long, storeMethod: Int): Iterator[Bytes] =
    StoreMethod.restoreCoder(storeMethod)(readRaw(dataid))

  private def readRaw(dataid: Long): Iterator[Bytes] =
    metaBackend.storeEntries(dataid).iterator
      .flatMap { case (start, fin) => dataBackend read (start, fin) }

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
