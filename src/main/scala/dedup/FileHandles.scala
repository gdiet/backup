package dedup

import java.io.File
import java.util.concurrent.Semaphore

import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/** The methods of this class are not thread safe. Synchronize externally if needed. */
class FileHandles(tempDir: File) {
  private val log = LoggerFactory.getLogger("dedup.FHand")

  /** file ID -> (handle count, cache entry) */
  private val entries = collection.mutable.Map[Long, (Int, CacheEntry)]()
  /** The number of concurrently allowed write processes (file system blocks when limit is reached). */
  private val writeProcesses = new Semaphore(3)

  def create(id: Long, ltsParts: Parts): Unit =
    require(entries.put(id, 1 -> new CacheEntry(ltsParts, tempDir)).isEmpty, s"entries already contained $id")

  def cacheEntry(id: Long): Option[CacheEntry] =
    entries.get(id).map(_._2)

  def incCount(id: Long): Unit =
    entries.updateWith(id) {
      case None => throw new IllegalArgumentException(s"entry $id not found")
      case Some(count -> entry) => Some(count + 1 -> entry)
    }

  /** @param onReleased Asynchronously executed callback. */
  def decCount(id: Long, onReleased: CacheEntry => Unit): Unit =
    entries.get(id) match {
      case None => throw new IllegalArgumentException(s"entry $id not found")
      case Some(1 -> entry) =>
        entries -= id
        if (!entry.dataWritten) entry.drop()
        else {
          // if there is data to persist, do it async, but limit number of active write processes
          Future {
            writeProcesses.acquire()
            try onReleased(entry)
            catch { case t: Throwable => log.error(s"onRelease failed for entry $id", t) }
            finally { writeProcesses.release(); entry.drop() }
          }(ExecutionContext.global)
        }
      case Some(count -> entry) => entries.update(id, count - 1 -> entry)
    }

  def delete(id: Long): Unit =
    entries.remove(id).foreach(_._2.drop())
}
