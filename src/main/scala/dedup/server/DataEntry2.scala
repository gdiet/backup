package dedup
package server

import java.util.concurrent.locks.ReentrantReadWriteLock as RWLock
import scala.util.Using.resource

final class DataEntry2 extends AutoCloseable:
  private val lock = RWLock()
  def acquire(): Unit = lock.readLock().lock()
  def release(): Unit = lock.readLock().unlock()

  override def close(): Unit =
    try
      lock.writeLock().lock()
      // FIXME clean up resources
    finally lock.writeLock().unlock()
