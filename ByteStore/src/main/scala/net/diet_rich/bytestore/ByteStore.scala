package net.diet_rich.bytestore

trait ByteStore {
  /** @return Data, where bytes not available in the store are set to zero */
  def read(from: Long, to: Long): Array[Byte]

  /** @return The current position if at a block start, else the position of the next block start */
  def nextBlockStart(position: Long): Long
  /** Write data to the store. */
  def write(data: Array[Byte], at: Long): Unit
  /** Set a data area to 'empty', trying to free storage (which simply writing zeros might not do). */
  def clear(from: Long, to: Long): Unit
}
