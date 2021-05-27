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

  /** Reads the requested number of bytes unless end-of-file is reached first,
    * in that case stops there.
    *
    * Unsafe: Should only be used if it is ensured that no writes to the DataEntry
    * occur during the read process.
    *
    * @param size Supports sizes larger than the internal size limit for byte arrays.
    * @return (actual size read, Vector(Either(holePosition, holeSize | position, bytes)) */
  def readUnsafe(offset: Long, size: Long): (Long, LazyList[Either[(Long, Long), (Long, Array[Byte])]]) = synchronized {
    val sizeToRead = math.max(0, math.min(size, cache.size - offset))
    sizeToRead -> cache.read(offset, sizeToRead)
  }

  /** Reads bytes from this [[DataEntry]] and writes them to `sink`.
    * Reads the requested number of bytes unless end-of-file is
    * reached first, in that case stops there. Returns the areas
    * not cached in this [[DataEntry]].
    *
    * Note: Providing a `sink` instead of returning the data
    * enables synchronized reads even though [[DataEntry]] is mutable
    * without incurring the risk of large memory allocations.
    *
    * @param offset       offset to start reading at.
    * @param size         number of bytes to read, NOT limited by the internal size limit for byte arrays.
    * @param sink         sink to write data to.
    *
    * @return (actual size read, Vector((holePosition, holeSize)))
    */
  def read[D: DataSink](offset: Long, size: Long, sink: D): (Long, Vector[(Long, Long)]) = synchronized {
    val (sizeRead, readResult) = readUnsafe(offset, size)
    sizeRead -> readResult.flatMap {
      case Left(hole) => Some(hole)
      case Right(position -> data) => sink.write(position - offset, data); None
    }.toVector
  }

end DataEntry

object DataEntry:
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get
end DataEntry
