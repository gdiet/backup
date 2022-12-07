package dedup
package server

/** Handler for the mutable contents of a virtual file. Does not need external synchronization. */
class DataEntry2(filePath: java.nio.file.Path, initialSize: Long) extends util.ClassLogging:
  private val cache = dedup.cache.WriteCache(filePath, initialSize)

  def size: Long = synchronized { cache.size }
  def written: Boolean = synchronized { cache.written }

  // For debugging.
  override def toString: String = synchronized { s"${getClass.getName}: id ${filePath.getFileName} / size $size / $cache" }

  /** Truncates the cached file to a new size. Zero-pads if the file size increases. */
  def truncate(newSize: Long): Unit = synchronized { cache.truncate(newSize) }

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. */
  def write(data: Iterator[(Long, Array[Byte])]): Unit = synchronized {
    data.foreach { (position, bytes) =>
      import Backend.cacheLoad
      if cacheLoad > 1000000000L then
        log.trace(s"Slowing write to reduce cache load $cacheLoad.")
        Thread.sleep(cacheLoad / 1000000000L)
      cache.write(position, bytes)
    }
  }

  /** Reads the requested number of bytes. Stops reading at the end of this [[DataEntry]].
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
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
