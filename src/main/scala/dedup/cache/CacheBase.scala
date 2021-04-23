package dedup.cache

import java.util

trait CacheBase[M] {
  implicit protected def m: MemArea[M]

  /** The methods are designed so no overlapping entries can occur. */
  protected var entries: util.NavigableMap[Long, M] = new util.TreeMap[Long, M]()

  def clear(position: Long, size: Long): Unit = {
    // If necessary, split lower entry.
    Option(entries.lowerEntry(position)).foreach { case Entry(storedPos, storedArea) =>
      val distance = position - storedPos
      if (storedArea.length > distance) {
        val (head, tail) = storedArea.split(distance)
        entries.put(storedPos, head)
        entries.put(storedPos + distance, tail)
      }
    }

    /** If necessary, trim ceiling entries.
      * @return `true` if trimming needs to be continued. */
    def trimAbove(): Boolean = Option(entries.ceilingEntry(position)).exists { case Entry(storedPos, storedArea) =>
      val overlap = position + size - storedPos
      if (overlap <= 0) {
        false
      } else if (overlap < storedArea.length) {
        entries.remove(storedPos)
        entries.put(storedPos + overlap, storedArea.drop(overlap))
        false
      } else {
        entries.remove(storedPos)
        storedArea.drop()
        true
      }
    }
    while(trimAbove()){/**/}
  }

  /** Truncates the managed areas to the provided size. */
  def keep(newSize: Long): Unit = {
    // Remove higher entries (by keeping all strictly lower entries).
    entries = entries.headMap(newSize, false)
    // If necessary, trim highest remaining entry.
    Option(entries.lastEntry()).foreach { case Entry(storedPosition, storedArea) =>
      val distance = newSize - storedPosition
      if (distance < storedArea.length) entries.put(storedPosition, storedArea.take(distance))
    }
  }

  protected def areasInSection(position: Long, size: Long): LazyList[Either[(Long, Long), (Long, M)]] = {
    {
      // Identify the relevant entries.
      val startKey = Option(entries.floorKey(position)).getOrElse(position)
      import scala.jdk.CollectionConverters.MapHasAsScala
      var section = entries.subMap(startKey, position + size - 1).asScala.toVector
      // Trim or remove the head entry if necessary.
      if (section.nonEmpty && startKey < position) {
        val (headPosition -> headData) +: tail = section
        val distance = position - headPosition
        if (headData.length <= distance) section = tail
        else section = (position -> headData.drop(distance)) +: tail
      }
      if (section.isEmpty) LazyList(Left(position -> size)) else {
        // Truncate the last entry if necessary.
        val lead :+ (tailPosition -> tailData) = section
        val distance = tailPosition + tailData.length - (position + size)
        if (distance > 0) section = lead :+ (tailPosition -> tailData.dropRight(distance))
        // Assemble result.
        def recurse(section: Vector[(Long, M)], currentPos: Long, remainingSize: Long): LazyList[Either[(Long, Long), (Long, M)]] =
          if (section.isEmpty) {
            if (remainingSize == 0) LazyList() else LazyList(Left(currentPos, remainingSize))
          } else {
            val (entry @ entryPos -> data) +: rest = section
            if (entryPos == currentPos)
              Right(entry) #:: recurse(rest, entryPos + data.length, remainingSize - data.length)
            else {
              val distance = entryPos - currentPos
              Left(currentPos, distance) #:: recurse(section, entryPos, remainingSize - distance)
            }
          }
        recurse(section, position, size)
      }
    }
  }
}
