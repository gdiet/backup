package dedup

object DataStoreResearch extends App {
  val blocks = 200
  val ds = new DataStoreResearch()
  System.gc()
  Thread.sleep(2000)
  val initialFreeMemory = Server.freeMemory
  println(s"*** initially free: ${initialFreeMemory / 1000000}")
  println()
  for (n <- 0 until blocks) {
    ds.write(n * 1048576L, new Array[Byte](1048576)) // 1048576 -> problem. 70 less -> ok
  }
  System.gc()
  Thread.sleep(2000)
  val freeMemoryWhenStored = Server.freeMemory
  println(s"*** free when stored: ${freeMemoryWhenStored / 1000000}")
  println(s"*** used up when stored: ${(initialFreeMemory - freeMemoryWhenStored) / 1000000}")
  println(s"*** expected mem usage : $blocks")
  println()
  ds.delete()
  System.gc()
  Thread.sleep(2000)
  val freeMemoryAfterDelete = Server.freeMemory
  println(s"*** free after delete: ${freeMemoryAfterDelete / 1000000}")
  println(s"*** used up after delete: ${(initialFreeMemory - freeMemoryAfterDelete) / 1000000}")
  println()
}

class DataStoreResearch() {
  private case class MemoryEntry(position: Long, data: Array[Byte]) {
    override def toString: String = s"Mem($position, $length)"
    def length: Int = data.length
    def memory: Long = length + 500
    def drop(left: Int, right: Int): MemoryEntry = copy(position = position + left, data = data.drop(left).dropRight(right))
    def write(data: Array[Byte]): Unit = System.arraycopy(data, 0, this.data, 0, data.length)
    def ++(other: MemoryEntry): MemoryEntry = copy(data = data ++ other.data)
  }
  private var entries = Option.empty[(Long, Seq[MemoryEntry])]
  private var memoryUsage = 0L

  def delete(writeLog: Boolean = true): Unit = {
    memoryUsage = memoryUsage - entries.toSeq.flatMap(_._2).map(_.memory).sum
    entries = None
  }

  def write(position: Long, data: Array[Byte]): Unit = {
    val (fileSize, chunks) = entries.getOrElse(0L -> Seq[MemoryEntry]())
    val newSize = math.max(fileSize, position + data.length)
    val combinedChunks = chunks :+ (chunks.find(_.position == position) match {
      case None => MemoryEntry(position, data)
      case Some(previousData) =>
        if (data.length >= previousData.length) MemoryEntry(position, data)
        else previousData.tap(_.write(data))
    })
    val (toMerge, others) = combinedChunks.partition { entry =>
      (entry.position < position && entry.position + entry.length > position) ||
        (entry.position >= position && entry.position < position + data.length)
    }
    val reduced = toMerge.sortBy(_.position).reduceLeft[MemoryEntry] { case (dataA, dataB) =>
      if (dataA.position + dataA.length >= dataB.position + dataB.length) dataA
      else dataA ++ dataB.drop((dataA.position + dataA.length - dataB.position).toInt, 0)
    }
    val merged = others :+ reduced
    delete(writeLog = false)
    memoryUsage += merged.map(_.memory).sum
    entries = Some(newSize -> merged.map {
      case m: MemoryEntry => m.copy(data = m.data.toArray)
      case o => o
    })
  }
}
