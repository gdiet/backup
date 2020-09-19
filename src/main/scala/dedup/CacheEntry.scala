package dedup

class CacheEntry(ltsParts: Parts) {
  var size: Long = 0
  var ltsSize: Long = ltsParts.size

  // Possible performance optimization: Use a sorted map, key is chunk position
  // Possible performance optimization: Merge adjacent file chunks
  private var chunks: Seq[Chunk] = Vector()

  private def dropFromChunks(from: Long, to: Long): Unit = {
    assert(from >= 0, s"negative from $from")
    assert(to > from, s"to $to not larger than from $from")
    chunks = chunks.flatMap { chunk =>
      val chunkEnd = chunk.position + chunk.size
      if (chunk.position >= to) Seq(chunk)
      else if (chunkEnd <= from) Seq(chunk)
      else {
        val leftOverlap = from - chunk.position
        val pre = if (leftOverlap > 0) Some(chunk.left(leftOverlap.toInt)) else None
        val rightOverlap = chunkEnd - to
        val post = if (rightOverlap > 0) Some(chunk.right(rightOverlap.toInt)) else None
        chunk.drop()
        Seq(pre, post).flatten
      }
    }
  }

  /** @param dataSource A function (offset, size) => data that provides the data to write. */
  def write(offset: Long, length: Long, dataSource: (Long, Int) => Array[Byte]): Unit = synchronized {
    val end = offset + length
    dropFromChunks(offset, end)
    for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) {
      val chunk = Chunk(position, dataSource(position, chunkSize))
      chunks :+= chunk
    }
    if (end > size) size = end
  }

  def truncate(length: Long): Unit = synchronized {
    assert(length >= 0, s"negative size $length")
    if (length < ltsSize) ltsSize = length
    if (size < length) dropFromChunks(length, Long.MaxValue)
    size = length
  }

  def dataWritten: Boolean = synchronized {
    chunks.nonEmpty || size != ltsSize || ltsSize != ltsParts.size
  }

  /** @param ltsRead A function (offset, size) => data that provides data from the long term store. */
  def read(start: Long, size: Long, ltsRead: (Long, Int) => Array[Byte]): LazyList[Array[Byte]] = ???
  /*
            val end = math.min(cacheEntry.size, offset + size)
            (for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) yield {
              val bytes = cacheEntry.read_old(position, chunkSize, ???)
              buf.put(position - offset, bytes, 0, bytes.length)
              chunkSize
            }).sum

   */
  /*
            val end = offset + size
            (for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) yield {
              val data = new Array[Byte](chunkSize)
              buf.get(position - offset, data, 0, chunkSize)
              cacheEntry.write(position, data)

            }).sum

            val data = new Array[Byte](intSize)
            store.write(file.id, file.dataId, db.dataSize(file.dataId))(offset, data)
            intSize

   */
  def drop(): Unit = ???
}
