package dedup2

import java.io.File

import scala.collection.mutable

class DataStore(repo: File, readOnly: Boolean) extends AutoCloseable {
  private val longTermStore = new LongTermStore(repo, readOnly)
  override def close(): Unit = longTermStore.close()
  private val basePath = new File(repo, "shortterm").getAbsolutePath
}
object DataStore {
  private val entries: mutable.Map[Long, mutable.SortedMap[Long, Array[Byte]]] = mutable.Map()
  private def entry(dataId: Long) =
    entries.getOrElse(dataId, mutable.SortedMap[Long, Array[Byte]]().tap(entries += dataId -> _))

  def read(dataId: Long, ltStart: Long, ltStop: Long)(position: Long, size: Int): Array[Byte] = ???
  def size(dataId: Long, ltStart: Long, ltStop: Long): Long = ???
  def write(dataId: Long)(position: Long, data: Array[Byte]): Unit = {
    val chunks = entry(dataId)
    println(s"Stage 1: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    chunks.get(position) match {
      case None => chunks += position -> data
      case Some(previousData) =>
        if (data.length >= previousData.length) chunks += position -> data
        else System.arraycopy(data, 0, previousData, 0, data.length)
    }
    println(s"Stage 2: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
    val endOfWrittenChunk = position + data.length
    val updates: mutable.Map[Long, Option[(Long, Array[Byte])]] = chunks.collect {
      case (chunkPosition, chunkData) if chunkPosition < position && chunkPosition + chunkData.length > position =>
        chunkPosition -> Some(chunkPosition -> chunkData.take((position - chunkPosition).toInt))
      case (chunkPosition, chunkData) if chunkPosition > position && chunkPosition < endOfWrittenChunk =>
        val remainingChunkSize = (chunkPosition + chunkData.length - endOfWrittenChunk).toInt
        if (remainingChunkSize < 1) chunkPosition -> None
        else chunkPosition -> Some(endOfWrittenChunk -> chunkData.takeRight(remainingChunkSize))
    }
    println(s"Updates: ${updates.view.mapValues(_.map(x => x._1 -> x._2.mkString("[",",","]"))).toMap}")
    updates.foreach { case (position, maybeNew) => chunks -= position; maybeNew.foreach(chunks +=) }
    println(s"Stage 3: ${chunks.view.mapValues(_.mkString("[",",","]")).toMap}")
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
