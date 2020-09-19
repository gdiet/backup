package dedup

class CacheEntry(ltsParts: Parts) {
  var size: Long = ltsParts.size
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

  /** @param dataSource A function (sourceOffset, size) => data that provides the data to write. */
  def write(offset: Long, length: Long, dataSource: (Long, Int) => Array[Byte]): Unit = synchronized {
    val end = offset + length
    dropFromChunks(offset, end)
    for (position <- offset until end by memChunk; chunkSize = math.min(memChunk, end - position).toInt) {
      val chunk = Chunk(position, dataSource(position - offset, chunkSize))
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
  def read(start: Long, length: Long, ltsRead: (Long, Int) => Array[Byte]): LazyList[Array[Byte]] = synchronized {
    assert(start >= 0, s"negative start $start")
    assert(length > 0, s"size $length not positive")
    val end = math.min(start + length, size)
    LazyList.range(start, end, memChunk).map { position =>
      val chunkSize = math.min(memChunk, end - position).toInt
      read(position, chunkSize, ltsRead)
    }
  }

  private def read(start: Long, length: Int, ltsRead: (Long, Int) => Array[Byte]): Array[Byte] = {
    assert(length > 0, s"length $length is not greater than zero")
    val end = start + length
    val sortedDataFromChunks = chunks.collect {
      case chunk if chunk.position >= start && chunk.position < end =>
        val take = math.min(chunk.size, (end - chunk.position).toInt)
        chunk.position -> chunk.data.take(take)
      case chunk if chunk.position <= start && chunk.end > start =>
        val drop = (start - chunk.position).toInt
        val dropRight = math.max(0, (chunk.end - end).toInt)
        chunk.position + drop -> chunk.data.drop(drop).dropRight(dropRight)
    }.sortBy(_._1)
    val (currentPosition, result) = sortedDataFromChunks.foldLeft((start, Array.empty[Byte])) {
      case ((position, result), (entryPosition, entryData)) if position == entryPosition =>
        (position + entryData.length, result ++ entryData)
      case ((position, result), (entryPosition, entryData)) =>
        assert(entryPosition > position, s"entryPosition $entryPosition less than position $position")
        val fromLts = readLtsOrZeros(position, (entryPosition - position).toInt, ltsRead)
        (entryPosition + entryData.length, result ++ fromLts ++ entryData)
    }
    result ++ readLtsOrZeros(currentPosition, (end - currentPosition).toInt, ltsRead)
  }

  /*
    def read(position: Long, size: Int, readLts: Part => Array[Byte]): Array[Byte] = synchronized {
      assert(size > 0, s"size $size is not greater than zero")
      val dataFromEntries = entries.collect {
        case e if e.position > position && e.position < position + size =>
          val take = math.min(e.size, (position + size - e.position).toInt)
          e.position -> e.data.take(take)
        case e if e.position <= position && e.position + e.size > position =>
          val drop = (position - e.position).toInt
          val dropRight = math.max(0, (e.position + e.size - position - size).toInt)
          e.position + drop -> e.data.drop(drop).dropRight(dropRight)
      }.sortBy(_._1)
      val (currentPosition, result) = dataFromEntries.foldLeft((0L, Array.empty[Byte])) {
        case ((position, result), (entryPosition, entryData)) =>
          assert(entryPosition >= position, s"entryPosition $entryPosition less than position $position")
          val fromLts = readLtsOrZeros(position, (entryPosition - position).toInt, readLts)
          (entryPosition + entryData.length, result ++ fromLts ++ entryData)
      }
      result ++ readLtsOrZeros(currentPosition, (position - currentPosition).toInt, readLts)
    }
   */

  private def readLtsOrZeros(start: Long, length: Int, ltsRead: (Long, Int) => Array[Byte]): Array[Byte] = {
    if (start >= ltsSize) new Array[Byte](length)
    else {
      val take = math.min(length, ltsSize - start).toInt
      val fill = length - take
      val read = ltsParts.drop(start).take(take).map { case (start, end) => ltsRead(start, (end - start).toInt) }
      val reduced = read.reduce(_ ++ _)
      if (fill > 0) reduced ++ new Array[Byte](fill) else reduced
    }
  }

  def drop(): Unit = synchronized {
    chunks.foreach(_.drop())
  }
}
