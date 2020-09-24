package _tryout

import dedup.DBMaintenance.endOfStorageAndDataGaps

import scala.collection.SortedMap

object Gaps extends App {
  val dataGaps = Vector[(Long, Long)](10L -> 30L, 60L -> 70L, 90L -> 100L)
  val length = 100

  val (availableLength, gapsToUse, remainingGaps) = dataGaps.foldLeft((0L, Vector[(Long, Long)](), Vector[(Long, Long)]())) {
    case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) if availableLength == length =>
      (availableLength, gapsToUse, remainingGaps.appended(cStart -> cStop))
    case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) =>
      val cLength = cStop - cStart
      if (availableLength + cLength <= length)
        (availableLength + cLength, gapsToUse.appended(cStart -> cStop), remainingGaps)
      else {
        val divideAt = cStart + length - availableLength
        (length, gapsToUse.appended(cStart -> divideAt), remainingGaps.appended(divideAt -> cStop))
      }
  }

  println(s"$availableLength $gapsToUse $remainingGaps")
}

object Gaps2 extends App {
  val dataEntries = Seq[(Long, Int, Long, Long)](
    (1, 1, 0, 10),
    (1, 2, 50, 60),
    (2, 1, 20, 40),
    (2, 2, 60, 70),
  )

  println(s"Number of data entries in storage database: ${dataEntries.size}")
  val dataChunks = dataEntries.map(e => e._3 -> e._4).to(SortedMap)
  println(s"Number of data chunks in storage database: ${dataChunks.size}")
  val (endOfStorage, dataGaps) = endOfStorageAndDataGaps(dataChunks)
  println(f"Current size of data storage: ${endOfStorage/1000000000d}%,.2f GB")
  val compactionPotential = dataGaps.map{ case (start, stop) => stop - start }.sum
  println(f"Compaction potential: ${compactionPotential/1000000000d}%,.2f GB in ${dataGaps.size} gaps.")

  println("\ngaps:\n" + dataGaps)

  val sortedEntries =
    dataEntries
      .groupBy(_._1)                                // group by id
      .view.mapValues { entries =>
      entries.head._1 ->                            // id -> ..
        entries.sortBy(_._2).map(e => e._3 -> e._4) // .. -> Seq(start, stop) ordered by seq
    }
      .values.toSeq
      .sortBy(-_._2.map(_._1).max)                  // order by stored last in lts

  println("\nsorted entries:")
  println(sortedEntries.mkString("\n"))

  val maybeEntry = sortedEntries.headOption
  val remainingEntries = sortedEntries.drop(1)
  val remainingGaps = dataGaps

  maybeEntry.foreach { case (id, chunks) =>
    val entrySize = chunks.map{ case (start, stop) => stop - start }.sum
    println(s"entrySize $entrySize for $id -> $chunks")

    val (compactionSize, gapsToUse, gapsNotUsed) =
      remainingGaps.foldLeft((0L, Vector[(Long, Long)](), Vector[(Long, Long)]())) {
        case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) if availableLength == entrySize =>
          (availableLength, gapsToUse, remainingGaps.appended(cStart -> cStop))
        case ((availableLength, gapsToUse, remainingGaps), (cStart, cStop)) =>
          val cLength = cStop - cStart
          if (availableLength + cLength <= entrySize)
            (availableLength + cLength, gapsToUse.appended(cStart -> cStop), remainingGaps)
          else {
            val divideAt = cStart + entrySize - availableLength
            (entrySize, gapsToUse.appended(cStart -> divideAt), remainingGaps.appended(divideAt -> cStop))
          }
      }

    println(s"compactionSize $compactionSize")
    println(s"gapsToUse $gapsToUse")
    println(s"gapsNotUsed $gapsNotUsed")

    if (compactionSize < entrySize || gapsToUse.last._2 > chunks.map(_._1).max) {
      if (compactionSize < entrySize)
        println("entry does not fit into available gaps.")
      else
        println("last gap used is beyond end of entry")
      println("no compaction possible, stopping...")
    } else {
      println("compaction possible, continuing...")
      println(s"1) store in lts at $gapsToUse")
      println(s"2) create new data entry for $gapsToUse")
      println(s"3) replace old data entry with new data entry in tree entries")
      println(s"4) recurse")
    }
    println(s"5) when finished, delete orphan data entries")
  }
}
