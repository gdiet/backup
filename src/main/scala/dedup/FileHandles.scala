package dedup

import java.io.File

import org.slf4j.LoggerFactory

/** The methods of this class are not thread safe. Synchronize externally if needed. */
class FileHandles(tempDir: File) {
  private val log = LoggerFactory.getLogger("dedup.FHand")

  /** file ID -> (handle count, cache entry) */
  private val entries = collection.mutable.Map[Long, (Int, CacheEntry)]()

  def create(id: Long, ltsParts: Parts): Unit =
    require(entries.put(id, 1 -> new CacheEntry(ltsParts, tempDir)).isEmpty, s"entries already contained $id")

  def createOrIncCount(id: Long, ltsParts: => Parts): Unit = {
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
    entries.get(id).map(_._2)

  /** @param onReleased (TODO Asynchronously) executed callback. */
  def decCount(id: Long, onReleased: CacheEntry => Unit): Unit =
    entries.get(id) match {
      case None => throw new IllegalArgumentException(s"entry $id not found")
      case Some(1 -> entry) =>
        log.trace(s"decCount() - drop handle for $id")
        entries.subtractOne(id)
        if (!entry.dataWritten) entry.drop()
        else
          // if there is data to persist, TODO do it async, but limit number of active write processes
          try onReleased(entry)
          catch { case t: Throwable => log.error(s"onRelease failed for entry $id", t) }
          finally entry.drop()
      case Some(count -> entry) =>
        log.trace(s"decCount() - decrement count to ${count-1} for $id")
        entries.update(id, count - 1 -> entry)
    }

  def delete(id: Long): Unit =
    entries.remove(id).foreach(_._2.drop())
}
