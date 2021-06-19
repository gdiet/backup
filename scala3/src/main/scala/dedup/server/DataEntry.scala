package dedup
package server

import DataEntry.{closedEntries, currentId}
import dedup.cache.{WriteCache, MemCache}
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

  private val path   = tempDir.resolve(s"$id")
  private val cache  = WriteCache(MemCache.availableMem, path, initialSize)
  private val isOpen = new CountDownLatch(1)

  def written: Boolean = synchronized(cache.written)
  def size: Long       = synchronized(cache.size   )

  /** Reads the requested number of bytes. Beyond end-of-file returns a hole entry.
    *
    * Unsafe: Use only if it is ensured that no writes to the DataEntry occur
    * during the read process while iterating the result lazy list.
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    *
    * @return LazyList(position, (holeSize | bytes) */
  def readUnsafe(offset: Long, size: Long): LazyList[(Long, Either[Long, Array[Byte]])] =
    cache.read(offset, size)

  /** Reads bytes from this [[DataEntry]] and writes them to `sink`.
    * Reads the requested number of bytes unless end-of-file is
    * reached first, in that case stops there. Returns the areas
    * not cached in this [[DataEntry]].
    *
    * Note: Providing a `sink` instead of returning the data
    * enables synchronized reads even though [[DataEntry]] is mutable
    * without incurring the risk of large memory allocations.
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    * @param sink   Sink to write data to.
    *
    * @return (actual size read, Vector((holePosition, holeSize))) */
  def read[D: DataSink](offset: Long, size: Long, sink: D): (Long, Vector[(Long, Long)]) = synchronized {
    val sizeToRead = math.max(0, math.min(size, cache.size - offset))
    sizeToRead -> readUnsafe(offset, sizeToRead).flatMap {
      case position -> Left(hole) => Some(position -> hole)
      case position -> Right(data) => sink.write(position - offset, data); None
    }.toVector
  }

object DataEntry:
  protected val currentId = new AtomicLong()
  protected val closedEntries = new AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get
