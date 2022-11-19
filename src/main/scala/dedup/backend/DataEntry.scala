package dedup
package backend

import dedup.cache.{MemCache, WriteCache}
import dedup.util.ClassLogging

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

/** Handler for the mutable contents of a virtual file. Does not need external synchronization. */
class DataEntry(idSeq: AtomicLong, initialSize: Long, tempDir: Path) extends ClassLogging:
  private val id        = idSeq.incrementAndGet()
  private val path      = tempDir.resolve(s"$id")
  private val cache     = WriteCache(path, initialSize)
  private def cacheLoad = WriteBackend.cacheLoad

  def size: Long       = synchronized { cache.size    }
  def written: Boolean = synchronized { cache.written }

  // For debugging.
  override def toString: String = synchronized { s"${getClass.getName}: id $id / size $size / $cache" }

  /** Truncates the cached file to a new size. Zero-pads if the file size increases. */
  def truncate(newSize: Long): Unit = synchronized { cache.truncate(newSize) }

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. */
  def write(data: Iterator[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { (position, bytes) =>
      if cacheLoad > 1000000000L then
        log.trace(s"Slowing write to reduce cache load $cacheLoad.")
        Thread.sleep(cacheLoad/1000000000L)
      cache.write(position, bytes)
    }
  }

  /** Reads the requested number of bytes. Stops reading at the end of this [[DataEntry]].
    * 
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    *
    * @return Iterator(position, holeSize | bytes). If writes occur to this [[DataEntry]] while the iterator is
    *         used, it is not defined which parts of the writes become visible and which parts don't.
    * @throws IllegalArgumentException if `offset` is negative.
    */
  def read(offset: Long, size: Long): Iterator[(Long, Either[Long, Array[Byte]])] =
    val sizeToRead = math.max(0, math.min(size, cache.size - offset))
    cache.read(offset, sizeToRead)

  def close(): Unit = synchronized {
    cache.close()
  }
