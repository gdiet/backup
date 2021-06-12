package dedup
package cache

trait CacheBase[M]:

  extension(m: M)
    protected def length: Long
    protected def dropped: Unit // FIXME make this the one "release" function, possibly by-ref
    protected def drop (distance: Long): M
    protected def keep (distance: Long): M
    protected def split(distance: Long): (M, M)

  // The methods are designed to avoid overlapping entries.
  protected var entries: java.util.NavigableMap[Long, M] = java.util.TreeMap[Long, M]()

  protected val markDropped = false

  /** Clears the specified part of the managed area. */
  def clear(position: Long, size: Long): Unit =
    require(position >= 0, s"Negative position: $position")
    require(size     >  0, s"Size not positive: $position")
    // If necessary, split lower entry.
    Option(entries.lowerEntry(position)).foreach { case JEntry(storedAt, area) =>
      val distance = position - storedAt
      if area.length > distance then
        val (head, tail) = area.split(distance)
        entries.put(storedAt, head)
        entries.put(storedAt + distance, tail)
    }

    // If necessary, drop or trim ceiling entries (including the higher part of the entry split before).
    while Option(entries.ceilingEntry(position)).exists { case JEntry(storedAt, area) =>
      val overlap = position + size - storedAt
      if overlap <= 0 then false
      else if (overlap < area.length) then
        entries.remove(storedAt)
        entries.put(storedAt + overlap, area.drop(overlap))
        false
      else
        entries.remove(storedAt)
        if markDropped then area.dropped
        true
    } do {/**/}

  /** Truncates the managed areas to the specified size. */
  def keep(newSize: Long): Unit =
    require(newSize >= 0, s"Negative new size: $newSize")
    // Remove higher entries.
    if markDropped then entries.tailMap(newSize).forEach((_, m) => m.dropped)
    // Keep all strictly lower entries.
    entries = java.util.TreeMap(entries.headMap(newSize)) // The headMap view doesn't accept out-of-range keys.
    // If necessary, trim highest remaining entry.
    Option(entries.lastEntry()).foreach { case JEntry(storedAt, area) =>
      val distance = newSize - storedAt
      if (distance < area.length) entries.put(storedAt, area.keep(distance))
    }

  /** For the specified area, return the list of data chunks and holes, ordered by position.
    *
    * @return LazyList(offset -> (holeSize | data)) where offset is relative to `position`. */
  protected def areasInSection(position: Long, size: Long): LazyList[(Long, Either[Long, M])] =
    // Identify the relevant entries.
    val startKey = Option(entries.floorKey(position)).getOrElse(position)
    import scala.jdk.CollectionConverters.MapHasAsScala
    var section = entries.subMap(startKey, position + size - 1).asScala.toVector
    // Trim or remove the head entry if necessary.
    if section.nonEmpty && startKey < position then
      val (headPosition -> headData) +: tail = section
      val distance = position - headPosition
      if headData.length <= distance then section = tail
      else section = (position -> headData.drop(distance)) +: tail
    //   if section.isEmpty then LazyList(Left(position -> size))
    //   else
    //     // Truncate the last entry if necessary.
    //     val lead :+ (tailPosition -> tailData) = section
    //     val distance = tailPosition + tailData.length - (position + size)
    //     if distance > 0 then section = lead :+ (tailPosition -> tailData.dropRight(distance))
    //     // Assemble result.
    //     def recurse(section: Vector[(Long, M)], currentPos: Long, remainingSize: Long): LazyList[Either[(Long, Long), (Long, M)]] =
    //       if section.isEmpty then
    //         if remainingSize == 0 then LazyList() else LazyList(Left(currentPos, remainingSize))
    //       else
    //         val (entry @ entryPos -> data) +: rest = section
    //         if entryPos == currentPos then
    //           Right(entry) #:: recurse(rest, entryPos + data.length, remainingSize - data.length)
    //         else
    //           val distance = entryPos - currentPos
    //           Left(currentPos, distance) #:: recurse(section, entryPos, remainingSize - distance)
    //     recurse(section, position, size)
    // }
    ???
