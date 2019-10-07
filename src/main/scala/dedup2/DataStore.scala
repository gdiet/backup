package dedup2

import scala.collection.immutable.SortedMap

class DataStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val longTermStore = new LongTermStore(dataDir, readOnly)
  override def close(): Unit = longTermStore.close()

  // Map(id/dataId -> size, Map(position, data))
  private var entries = Map[(Long, Long), (Long, Map[Long, Array[Byte]])]()

  def hasTemporaryData(id: Long, dataId: Long): Boolean = entries.contains(id -> dataId)

  def read(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, requestedSize: Int): Array[Byte] = {
    val (fileSize, localChunks) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Map[Long, Array[Byte]]())
    val sizeToRead = math.max(math.min(requestedSize, fileSize - position), 0).toInt
    val chunks: Map[Long, Array[Byte]] = localChunks.collect {
      case (chunkPosition, chunkData) if chunkPosition < position && chunkPosition + chunkData.length > position =>
        position -> chunkData.drop((position - chunkPosition).toInt)
      case (chunkPosition, chunkData) if chunkPosition >= position && chunkPosition < position + sizeToRead =>
        val drop = math.max(0, chunkPosition + chunkData.length - (position + sizeToRead)).toInt
        chunkPosition -> chunkData.dropRight(drop)
    }
    // println(s"READ chunks: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val chunksToRead = chunks.foldLeft(SortedMap(position -> sizeToRead)) { case (chunksToRead, (chunkPosition, chunkData)) =>
      // println(s"READ chunksToRead -> $chunksToRead")
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= chunkPosition).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (chunkPosition - readPosition).toInt,
        chunkPosition + chunkData.length -> (readPosition + sizeToRead - chunkPosition - chunkData.length).toInt
      ).filterNot(_._2 == 0)
    }
    // println(s"READ chunksToRead: $chunksToRead")
    val chunksRead: SortedMap[Long, Array[Byte]] =
      chunksToRead.map { case (start, length) => start -> longTermStore.read(ltStart + start, length) }
    // println(s"READ chunksRead: ${chunksRead.view.mapValues(_.mkString("[",",","]")).toMap}")
    (chunksRead ++ chunks).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Long =
    entries.get(id -> dataId).map(_._1).getOrElse(ltStop - ltStart)

  def delete(id: Long, dataId: Long): Unit = entries -= (id -> dataId)

  def truncate(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Unit =
    entries += (id -> dataId) -> (0L -> Map())

  def write(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getOrElse(id -> dataId, ltStop - ltStart -> Map[Long, Array[Byte]]())
    val newSize = math.max(fileSize, position + data.length)
    // println(s"\nStage 1: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val combinedChunks = chunks + (chunks.get(position) match {
      case None => position -> data
      case Some(previousData) =>
        if (data.length >= previousData.length) position -> data
        else { System.arraycopy(data, 0, previousData, 0, data.length); position -> previousData }
    })
    // println(s"Stage 2: ${combinedChunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val (toMerge, others) = combinedChunks.partition { case (chunkPosition, chunkData) =>
      (chunkPosition < position && chunkPosition + chunkData.length > position) ||
        (chunkPosition >= position && chunkPosition < position + data.length)
    }
    // println(s"Merge  : ${toMerge.view.mapValues(_.mkString("[",",","]")).toMap}")
    val reduced = toMerge.toSeq.sortBy(_._1).reduceLeft[(Long, Array[Byte])] { case (posA -> dataA, posB -> dataB) =>
      if (posA + dataA.length >= posB + dataB.length) posA -> dataA
      else posA -> (dataA ++ dataB.drop((posA + dataA.length - posB).toInt))
    }
    // println(s"Reduced: ${reduced._1} ${reduced._2.mkString("[",",","]")}")
    val merged = others + reduced
    // println(s"Stage 3: ${merged.view.mapValues(_.mkString("[",",","]")).toMap}")
    entries += (id -> dataId) -> (newSize -> merged)
  }
}
