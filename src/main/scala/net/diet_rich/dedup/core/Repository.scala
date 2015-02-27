package net.diet_rich.dedup.core

import java.io.IOException

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.meta.{TreeEntry, RangesQueue, MetaBackend}
import net.diet_rich.dedup.util.now

object Repository {
  val PRINTSIZE = 8192
}

class Repository(metaBackend: MetaBackend, dataBackend: DataBackend, freeRanges: RangesQueue, hashAlgorithm: String, storeMethod: Int, storeThreads: Int) {

  protected val storeLogic: StoreLogicBackend = new StoreLogic(metaBackend, dataBackend.write _, freeRanges, hashAlgorithm, storeMethod, storeThreads)

  def close(): Unit = {
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
