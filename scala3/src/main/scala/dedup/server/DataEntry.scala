package dedup.server

import DataEntry.{closedEntries, currentId}
import dedup.cache.{CombinedCache, MemCache}
import dedup.util.ClassLogging

import java.util.concurrent.atomic.AtomicLong
import java.nio.file.Path
import java.util.concurrent.CountDownLatch

/** Thread safe handler for the mutable contents of a virtual file.
  *
  * @param baseDataId Id of the data record this entry updates. -1 if this entry is independent. */
class DataEntry(val baseDataId: AtomicLong, initialSize: Long, tempDir: Path) extends ClassLogging:

  val id: Long = currentId.incrementAndGet()
  log.trace(s"Create $id with base data ID $baseDataId.")

  private val path = tempDir.resolve(s"$id")
  private val cache = CombinedCache(MemCache.availableMem, path, initialSize)
  private val isOpen = new CountDownLatch(1)

  def written: Boolean = synchronized(cache.written)
  def size: Long       = synchronized(cache.size   )

end DataEntry

object DataEntry:
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get
end DataEntry
