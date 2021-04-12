package dedup

import dedup.DataEntry.{availableMem, closedEntries, currentId}
import dedup.cache.CombinedCache

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

object DataEntry {
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get

  val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val availableMem = new AtomicLong(cacheLimit)
}

/** Thread safe handler for the mutable contents of a virtual file.
  *
  * @param baseDataId Id of the data record this entry updates. -1 if this entry is independent. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable with ClassLogging {
  private val id = currentId.incrementAndGet()
  log.trace(s"Create $id with base data ID $baseDataId.")

  private val path = tempDir.resolve(s"$id")
  private val cache = new CombinedCache(availableMem, path, initialSize)

  def written: Boolean = synchronized(cache.written)
  def size: Long = synchronized(cache.size)

  /** @param size Supports sizes larger than the internal size limit for byte arrays.
    * @param sink The sink to write data into. Providing this instead of returning the data read reduces memory
    *             consumption, especially in case of large reads, while allowing atomic / synchronized reads.
    * @return Some(Vector((holePosition, holeSize)))
    *         or None if the request exceeds the entry size. */
  def read[D: DataSink](offset: Long, size: Int, sink: D): Option[Vector[(Long, Long)]] = synchronized {
    // Materialize the lazy entries in the synchronized context to avoid concurrency issues.
    // This is safe from memory point of view because size is restricted to the internal size limit for byte arrays.
//    cache.read(offset, size).map(_.toVector)
    ???
  }

  def truncate(size: Long): Unit = synchronized { cache.truncate(size) }

  /** @param data LazyList(position -> bytes). Providing the complete data as LazyList allows running the update
    *             atomically / synchronized. */
  def write(data: LazyList[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { case (position, bytes) => cache.write(position, bytes) }
  }

  override def close(): Unit = synchronized {
    cache.close()
    log.trace(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
  }
}
