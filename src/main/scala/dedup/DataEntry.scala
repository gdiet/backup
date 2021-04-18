package dedup

import dedup.DataEntry.{availableMem, closedEntries, currentId}
import dedup.cache.CombinedCache

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

object DataEntry {
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get

  // TODO move to MemCache
  val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  val availableMem = new AtomicLong(cacheLimit)
}

/** Thread safe handler for the mutable contents of a virtual file.
  *
  * @param baseDataId Id of the data record this entry updates. -1 if this entry is independent. */
class DataEntry(val baseDataId: Long, initialSize: Long, tempDir: Path) extends AutoCloseable with ClassLogging {
  val id: Long = currentId.incrementAndGet()
  log.trace(s"Create $id with base data ID $baseDataId.")

  private val path = tempDir.resolve(s"$id")
  private val cache = new CombinedCache(availableMem, path, initialSize)

  def written: Boolean = synchronized(cache.written)
  def size: Long = synchronized(cache.size)

  /** @param size Supports sizes larger than the internal size limit for byte arrays.
    * @param sink The sink to write data into. Providing this instead of returning the data read reduces memory
    *             consumption in case of large reads while allowing atomic / synchronized reads.
    * @return (actual size read, Vector((holePosition, holeSize))) */
  def read[D: DataSink](offset: Long, size: Long, sink: D): (Long, Vector[(Long, Long)]) = synchronized {
    val (sizeRead, readResult) = readUnsafe(offset, size)
    sizeRead -> readResult.flatMap {
      case Left(hole) => Some(hole)
      case Right(position -> data) => sink.write(position - offset, data); None
    }.toVector
  }

  /** Unsafe: Should only be used if it is ensured that no writes to the DataEntry occur during the read process.
    *
    * @param size Supports sizes larger than the internal size limit for byte arrays.
    * @return (actual size read, Vector(Either(holePosition, holeSize | position, bytes)) */
  def readUnsafe(offset: Long, size: Long): (Long, LazyList[Either[(Long, Long), (Long, Array[Byte])]]) = synchronized {
    val sizeToRead = math.min(size, cache.size - offset)
    sizeToRead -> cache.read(offset, sizeToRead)
  }

  def truncate(size: Long): Unit = synchronized { cache.truncate(size) }

  /** @param data LazyList(position -> bytes). Providing the complete data as LazyList allows running the update
    *             atomically / synchronized. */
  def write(data: LazyList[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { case (position, bytes) =>
      if (Level2.cacheLoad > 1000000000L) {
        log.trace(s"Slowing write to reduce cache load ${Level2.cacheLoad}.")
        Thread.sleep(Level2.cacheLoad/1000000000L)
      }
      cache.write(position, bytes)
    }
  }

  override def close(): Unit = synchronized {
    cache.close()
    log.trace(s"Close $id with base data ID $baseDataId.")
    closedEntries.incrementAndGet()
  }
}
