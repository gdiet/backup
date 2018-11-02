package net.diet_rich.bytestore

class MemoryByteStore extends ByteStore {
  val data = new Array[Byte](32000000)

  /** @return Data, where bytes not available in the store are set to zero */
  override def read(from: Long, to: Long): Array[Byte] = {
    val bytes = new Array[Byte]((to-from).toInt)
    System.arraycopy(data, from.toInt, bytes, 0, (to-from).toInt)
    bytes
  }

  /** @return The current position if at a block start, else the position of the next block start */
  override def nextBlockStart(position: Long): Long =
    if (position % 1000 == 0) position else position + 1000 - position % 1000

  /** Write data to the store. */
  override def write(bytes: Array[Byte], at: Long): Unit =
    System.arraycopy(bytes, 0, data, at.toInt, bytes.length)

  /** Set a data area to 'empty', trying to free storage (which simply writing zeros might not do). */
  override def clear(from: Long, to: Long): Unit =
    System.arraycopy(new Array[Byte]((to-from).toInt), 0, data, from.toInt, (to-from).toInt)
}
