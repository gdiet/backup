package dedup2

import scala.collection.immutable.SortedMap

class DataStore(dataDir: String, readOnly: Boolean) extends AutoCloseable {
  private val longTermStore = new LongTermStore(dataDir, readOnly)
  override def close(): Unit = longTermStore.close()

  private var entries = Map[(Long, Long), Map[Long, Array[Byte]]]().withDefaultValue(Map())

  def read(id: Long, dataId: Long, ltStart: Long, ltStop: Long)(position: Long, requestedSize: Int): Array[Byte] = {
    val size = math.max(math.min(requestedSize, this.size(id, dataId, ltStart, ltStop) - position), 0).toInt
    val chunks: Map[Long, Array[Byte]] = entries(id -> dataId).collect {
      case (chunkPosition, chunkData) if chunkPosition < position && chunkPosition + chunkData.length > position =>
        position -> chunkData.drop((position - chunkPosition).toInt)
      case (chunkPosition, chunkData) if chunkPosition >= position && chunkPosition < position + size =>
        val drop = math.max(0, chunkPosition + chunkData.length - (position + size)).toInt
        chunkPosition -> chunkData.dropRight(drop)
    }
    println(s"READ chunks: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val chunksToRead = chunks.foldLeft(SortedMap(position -> size)) { case (chunksToRead, (chunkPosition, chunkData)) =>
      println(s"READ chunksToRead -> $chunksToRead")
      val (readPosition, sizeToRead) = chunksToRead.filter(_._1 <= chunkPosition).last
      chunksToRead - readPosition ++ Seq(
        readPosition -> (chunkPosition - readPosition).toInt,
        chunkPosition + chunkData.length -> (readPosition + sizeToRead - chunkPosition - chunkData.length).toInt
      ).filterNot(_._2 == 0)
    }
    println(s"READ chunksToRead: $chunksToRead")
    val chunksRead: SortedMap[Long, Array[Byte]] =
      chunksToRead.map { case (start, length) => start -> longTermStore.read(ltStart + start, length) }
    println(s"READ chunksRead: ${chunksRead.view.mapValues(_.mkString("[",",","]")).toMap}")
    (chunksRead ++ chunks).map(_._2).reduce(_ ++ _)
  }

  def size(id: Long, dataId: Long, ltStart: Long, ltStop: Long): Long =
    math.max(ltStop - ltStart, entries(id -> dataId).maxByOption(_._1).map(e => e._1 + e._2.length).getOrElse(0L))

  def write(id: Long, dataId: Long)(position: Long, data: Array[Byte]): Unit = {
    val chunks = entries(id -> dataId)
    println(s"\nStage 1: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val combinedChunks = chunks + (chunks.get(position) match {
      case None => position -> data
      case Some(previousData) =>
        if (data.length >= previousData.length) position -> data
        else { System.arraycopy(data, 0, previousData, 0, data.length); position -> previousData }
    })
    println(s"Stage 2: ${combinedChunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val (toMerge, others) = combinedChunks.partition { case (chunkPosition, chunkData) =>
      (chunkPosition < position && chunkPosition + chunkData.length > position) ||
        (chunkPosition >= position && chunkPosition < position + data.length)
    }
    println(s"Merge  : ${toMerge.view.mapValues(_.mkString("[",",","]")).toMap}")
    val reduced = toMerge.toSeq.sortBy(_._1).reduceLeft[(Long, Array[Byte])] { case (posA -> dataA, posB -> dataB) =>
      if (posA + dataA.length >= posB + dataB.length) posA -> dataA
      else posA -> (dataA ++ dataB.drop((posA + dataA.length - posB).toInt))
    }
    println(s"Reduced: ${reduced._1} ${reduced._2.mkString("[",",","]")}")
    val merged = others + reduced
    println(s"Stage 3: ${merged.view.mapValues(_.mkString("[",",","]")).toMap}")
    entries += (id -> dataId) -> merged
  }
}
