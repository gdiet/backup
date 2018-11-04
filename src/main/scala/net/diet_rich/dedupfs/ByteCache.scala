package net.diet_rich.dedupfs

import net.diet_rich.util.init

class ByteCache {
  private var data: Map[Long, Array[Byte]] = Map()

  def write(id: Long, bytes: Array[Byte], offset: Long): Unit = {
    def newChunk = new Array[Byte]((offset + bytes.length).toInt)
    val chunk = data.getOrElse(id, newChunk)
    val extended =
      if (chunk.length >= offset + bytes.length) chunk
      else init(newChunk)(System.arraycopy(chunk, 0, _, 0, chunk.length))
    System.arraycopy(bytes, 0, extended, offset.toInt, bytes.length)
    data += id -> extended
  }

  def release(id: Long): Unit = data -= id

  def read(id: Long): Option[Stream[Array[Byte]]] = data.get(id).map(bytes => Stream(bytes))
}
