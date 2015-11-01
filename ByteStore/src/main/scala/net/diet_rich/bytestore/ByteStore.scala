package net.diet_rich.bytestore

import net.diet_rich.common.Bytes

trait ByteStore extends ByteStoreRead {
  /** Write data to the store. */
  def write(data: Bytes, at: Long): Unit
  /** Set a data area to 'empty', trying to free storage (which simply writing might not do). */
  def clear(from: Long, to: Long): Unit
}

trait ByteStoreRead extends AutoCloseable {
  // FIXME why for read? maybe this is only needed for writing...
  /** @return The current position if at a block start, else the position of the next block start */
  def nextBlockStart(position: Long): Long
  /** @return Data, where bytes not available in the store are set to zero */
  def read(from: Long, to: Long): Iterator[Bytes]
}

/** Extension allowing to convert byte stores on the fly. */
trait ByteStoreReadRaw extends AutoCloseable {
  /** @return The bytes that are in fact available in the store and the sizes of the gaps in between */
  def readRaw(from: Long, to: Long): Iterator[Either[Int, Bytes]]
}
