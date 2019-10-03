package dedup2

import java.io.File

class DataStore(repo: File, readOnly: Boolean) extends AutoCloseable {
  private val longTermStore = new LongTermStore(repo, readOnly)
  override def close(): Unit = longTermStore.close()
  private val basePath = new File(repo, "shortterm").getAbsolutePath
}
object DataStore {
  private var entries = Map[Long, Map[Long, Array[Byte]]]().withDefaultValue(Map())

  def read(dataId: Long, ltStart: Long, ltStop: Long)(position: Long, size: Int): Array[Byte] = ???

  def size(dataId: Long, ltStart: Long, ltStop: Long): Long =
    math.max(ltStop - ltStart, entries(dataId).maxByOption(_._1).map(e => e._1 + e._2.length).getOrElse(0))

  def write(dataId: Long)(position: Long, data: Array[Byte]): Unit = {
    val chunks = entries(dataId)
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
    entries += dataId -> merged
  }
}

object X extends App {
  DataStore.write(1)(10, Array[Byte](3, 1, 8, 8, 3, 1))
  println()
  DataStore.write(1)(8, Array[Byte](3, 1, 8, 8, 3, 1))
  println()
  DataStore.write(1)(0, Array[Byte](3, 1, 8, 8, 3, 1))
  println()
  DataStore.write(1)(12, Array[Byte](3, 1, 8, 8, 3, 1))
}
