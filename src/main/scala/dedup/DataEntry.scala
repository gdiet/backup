package dedup

import dedup.DataEntry.{availableMem, closedEntries, currentId}
import dedup.cache.{CombinedCache, DataSink}
import jnr.ffi.Pointer

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

object DataEntry {
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get

  val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val availableMem = new AtomicLong(cacheLimit)
}

/** The internal cache is mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable with ClassLogging {
  private val id = currentId.incrementAndGet()
  log.trace(s"Create $id with base data ID $baseDataId.")

  private val path = tempDir.resolve(s"$id")
  private val cache = new CombinedCache(availableMem, path, initialSize)

  def written: Boolean = synchronized(cache.written)
  def size: Long = synchronized(cache.size)

  /** @return None if request exceeds available size or Some(holes to fill). */
  def read[D: DataSink](offset: Long, size: Int, sink: D): Option[Vector[(Long, Long)]] = synchronized {
    if (offset + size > cache.size) None else Some(cache.read(offset, size, sink))
  }

  def truncate(size: Long): Unit = synchronized { cache.truncate(size) }

  def write(offset: Long, size: Long, source: Pointer): Unit = synchronized {
    for (position <- 0L until size by memChunk; chunkSize = math.min(memChunk, size - position).toInt) {
      val chunk = new Array[Byte](chunkSize)
      source.get(position, chunk, 0, chunkSize)
      cache.write(offset + position, chunk)
    }
  }

  override def close(): Unit = synchronized {
    cache.close()
    log.trace(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
  }
}
