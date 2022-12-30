package dedup
package server

/** Thread safe handler for the mutable contents of a virtual file. */
final class DataEntry(idSeq: java.util.concurrent.atomic.AtomicLong, initialSize: Long, tempDir: java.nio.file.Path)
  extends AutoCloseable with util.ClassLogging:
  private val id    = idSeq.incrementAndGet()
  private val path  = tempDir.resolve(s"$id")
  private val cache = dedup.cache.WriteCache(path, initialSize)

  private val lock = java.util.concurrent.locks.ReentrantReadWriteLock()
  def acquire(): Unit = lock.readLock().lock()
  def release(): Unit = lock.readLock().unlock()

  /** Reads the requested number of bytes. Stops reading at the end of this [[DataEntry]].
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    * @return Iterator(position, holeSize | bytes). If writes occur to this [[DataEntry]] while the iterator is
    *         used, it is not defined which parts of the writes become visible and which parts don't.
    * @throws IllegalArgumentException if `offset` is negative.
    */
  def read(offset: Long, size: Long): Iterator[(Long, Either[Long, Array[Byte]])] =
    synchronized { cache.read(offset, size) }

  /** @param data Iterator(position -> bytes). The write is executed atomically / synchronized. */
  def write(data: Iterator[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { (position, bytes) =>
      if Backend.cacheLoadDelay > 0 then // TODO consider moving this to Backend
        log.trace(s"Slowing write by ${Backend.cacheLoadDelay} ms due to high cache load.")
        Thread.sleep(Backend.cacheLoadDelay)
      cache.write(position, bytes)
    }
  }

  def truncate(newSize: Long): Unit = synchronized { cache.truncate(newSize) }

  def size: Long = synchronized { cache.size }

  override def close(): Unit =
    lock.writeLock().lock()
    cache.close()
