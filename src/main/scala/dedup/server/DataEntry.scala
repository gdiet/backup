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
  def getBaseDataId: DataId = DataId(baseDataId.get())

  val id: Long = currentId.incrementAndGet()
  log.trace(s"Create $id with base data ID $baseDataId.")

  private val path   = tempDir.resolve(s"$id")
  private val cache  = WriteCache(path, initialSize)
  private val isOpen = CountDownLatch(1)

  def written: Boolean = synchronized(cache.written)
  def size: Long       = synchronized(cache.size   )

  /** For debugging purposes. */
  override def toString: String = synchronized {
    s"${getClass.getName}: id $id / size $size / $cache"
  }

  def truncate(newSize: Long): Unit = synchronized { cache.truncate(newSize) }

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. */
  def write(data: Iterator[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { (position, bytes) =>
      if Level2.cacheLoadDelay > 0 then
        log.trace(s"Slowing write by ${Level2.cacheLoadDelay} ms due to high cache load.")
        Thread.sleep(Level2.cacheLoadDelay)
      cache.write(position, bytes)
    }
  }

  /** Reads the requested number of bytes. Stops reading at the end of this [[DataEntry]].
    *
    * Unsafe: Use only if it is ensured that no writes to the DataEntry occur
    * during the read process while iterating the result lazy list.
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    *
    * @return Iterator(position, holeSize | bytes)
    * @throws IllegalArgumentException if `offset` / `size` exceed the bounds of the virtual file.
    */
  def readUnsafe(offset: Long, size: Long): Iterator[(Long, Either[Long, Array[Byte]])] =
    cache.read(offset, size)

  /** Reads bytes from this [[DataEntry]] and writes them to `sink`.
    * Stops reading at the end of this [[DataEntry]]. Returns the areas
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
    sizeToRead -> cache.read(offset, sizeToRead).flatMap {
      case position -> Left(hole) => Some(position -> hole)
      case position -> Right(data) => sink.write(position - offset, data); None
    }.toVector
  }

  def close(finalDataId: DataId): Unit = synchronized {
    cache.close()
    closedEntries.incrementAndGet()
    isOpen.countDown()
    baseDataId.set(finalDataId.asLong)
    log.trace(s"Closed $id with new base data ID $baseDataId.")
  }

  def awaitClosed(): Unit = isOpen.await()

object DataEntry:
  protected val currentId    : AtomicLong = AtomicLong()
  protected val closedEntries: AtomicLong = AtomicLong()
  def openEntries: Long = currentId.get - closedEntries.get
