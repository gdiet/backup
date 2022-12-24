package dedup
package server

import java.util.concurrent.locks.ReentrantReadWriteLock as RWLock

final class DataEntry2 extends AutoCloseable:
  private val lock = RWLock()
  def acquire(): Unit = lock.readLock().lock()
  def release(): Unit = lock.readLock().unlock()

  /** Reads the requested number of bytes. Stops reading at the end of this [[DataEntry2]].
    *
    * @param offset Offset to start reading at.
    * @param size   Number of bytes to read, not limited by the internal size limit for byte arrays.
    * @return Iterator(position, holeSize | bytes). If writes occur to this [[DataEntry2]] while the iterator is
    *         used, it is not defined which parts of the writes become visible and which parts don't.
    * @throws IllegalArgumentException if `offset` is negative.
    */
  def read(offset: Long, size: Long): Iterator[(Long, Either[Long, Array[Byte]])] =
    ???

  def write(data: Iterator[(Long, Array[Byte])]): Unit =
    ???
  
  def size: Long = ???

  override def close(): Unit =
    try
      lock.writeLock().lock()
      // FIXME clean up resources
    finally lock.writeLock().unlock()
