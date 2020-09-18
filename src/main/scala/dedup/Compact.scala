package dedup

object Compact extends App {

  def findCompaction(sourceStart: Long, sourceEnd: Long, freeChunks: Seq[(Long, Long)]): Option[(List[(Long, Long)], List[(Long, Long)])] = {
    val availableChunks = freeChunks.filter { case (from, to) => to > from && to <= sourceStart }
    val availableSpace = availableChunks.map { case (from, to) => to - from }.sum
    val sourceSize = sourceEnd - sourceStart
    if (availableSpace < sourceSize)
      None
    else {
      val (_, target, remaining) = availableChunks.foldLeft[(Long, List[(Long, Long)], List[(Long, Long)])]((0L, List(), List())) {
        case ((length, target, remaining), chunk @ (chunkStart, chunkEnd)) =>
          val chunkSize = chunkEnd - chunkStart
          if (sourceSize > length) {
            if (sourceSize > length + chunkSize)
              (length + chunkSize, chunk :: target, remaining)
            else {
              val partSize = sourceSize - length
              (length + chunkSize, (chunkStart, chunkStart + partSize) :: target, (chunkStart + partSize, chunkEnd) :: remaining)
            }
          } else
            (length + chunkSize, target, chunk :: remaining)
      }
      Some(target -> remaining)
    }
  }

  val (target, remaining) = findCompaction(
    1000, 1050, Seq(
      (0, 0),
      (5, 20),
      (55, 90),
      (995, 1005),
      (2000, 3000)
    )
  ).get

  println(s"target: $target")
  println(s"remaining: $remaining")
}
