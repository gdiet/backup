package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.data._
import net.diet_rich.dedup.core.meta.{RangesQueue, MetaBackend}

object Repository {
  val PRINTSIZE = 8192
}

class Repository(metaBackend: MetaBackend, dataBackend: DataBackend, freeRanges: RangesQueue, hashAlgorithm: String, storeMethod: Int, storeThreads: Int) {

  val storeLogic: StoreLogicBackend = new StoreLogic(metaBackend, ???, freeRanges, hashAlgorithm, storeMethod, storeThreads)

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

}
