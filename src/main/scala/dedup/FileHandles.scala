package dedup

import java.io.File

import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class FileHandles(tempDir: File) {
  private val log = LoggerFactory.getLogger("dedup.FHand")
  private def sync[T](f: => T): T = synchronized(f)

  private val maxEntries = new java.util.concurrent.Semaphore(3)

  /** file ID -> (handle count, cache entry) */
  private val entries = collection.mutable.Map[Long, (Int, CacheEntry)]()

  def create(id: Long, ltsParts: Parts): Unit = {
    maxEntries.acquire() // outside the sync block to avoid possible deadlocks
    sync(require(entries.put(id, 1 -> new CacheEntry(ltsParts, tempDir)).isEmpty, s"entries already contained $id"))
  }

  def createOrIncCount(id: Long, ltsParts: => Parts): Unit = sync {
    entries.get(id) match {
      case None =>
        log.trace(s"createOrIncCount() - create $id")
        create(id, ltsParts)
      case Some(count -> entry) =>
        log.trace(s"createOrIncCount() - increment count to ${count+1} for $id")
        entries.update(id, count + 1 -> entry)
    }
  }

  def cacheEntry(id: Long): Option[CacheEntry] =
    sync(entries.get(id)).map(_._2)

  /** @param onReleased Asynchronously executed callback. */
  def decCount(id: Long, onReleased: CacheEntry => Unit): Unit = sync {
    entries.get(id) match {
      case None => throw new IllegalArgumentException(s"entry $id not found")
      case Some(1 -> entry) =>
        log.trace(s"decCount() - drop handle for $id")
        entries.subtractOne(id)
        Some(entry)
      case Some(count -> entry) =>
        log.trace(s"decCount() - decrement count to ${count-1} for $id")
        entries.update(id, count - 1 -> entry)
        None
    }
  }.foreach { entry =>
    if (!entry.dataWritten) entry.drop()
    else Future {
      try onReleased(entry)
      catch { case t: Throwable => log.error(s"onRelease failed for entry $id", t) }
      finally { maxEntries.release(); entry.drop() }
    }(ExecutionContext.global)
  }

  def delete(id: Long): Unit =
    sync(entries.remove(id)).foreach { case (_, entry) => maxEntries.release(); entry.drop() }
}
