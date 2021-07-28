package dedup
package cache

import scala.jdk.CollectionConverters.MapHasAsScala

trait LongCache extends CacheBase[Long]:
  override protected def length(m: Long): Long = m
  override protected def merge (m: Long, n       : Long): Option[Long] = Some(m + n)
  override protected def drop  (m: Long, distance: Long): Long         = m - distance
  override protected def keep  (m: Long, distance: Long): Long         = distance
  override protected def split (m: Long, distance: Long): (Long, Long) = (distance, m - distance)

trait CacheBase[M]:
  protected def length(m: M): Long
  protected def merge (m: M, n: M): Option[M]
  protected def drop  (m: M, distance: Long): M
  protected def keep  (m: M, distance: Long): M
  protected def split (m: M, distance: Long): (M, M)

  /** Called when entries are (partially) removed from cache. Overridden by [[MemCache]] for memory management. */
  protected def release(sizes: => Iterable[Long]): Unit = {/**/}

  extension(m: M)
    private def _check(distance: Long) = require(distance > 0 && distance < length(m), s"Distance: $distance")
    private def _length: Long = length(m)
    private def _merge(other   : M   ): Option[M] = merge(m, other)
    private def _drop (distance: Long): M         = { _check(distance); drop (m, distance) }
    private def _keep (distance: Long): M         = { _check(distance); keep (m, distance) }
    private def _split(distance: Long): (M, M)    = { _check(distance); split(m, distance) }

  // The methods are designed to avoid overlapping entries.
  protected var entries: java.util.NavigableMap[Long, M] = java.util.TreeMap[Long, M]()

  /** For debugging purposes. */
  override def toString: String =
    s"$getClass ${entries.asScala.view.mapValues(_._length).toMap.toString}"

  /** Clears the specified part of the managed area. */
  def clear(position: Long, size: Long): Unit =
    require(position >= 0, s"Negative position: $position")
    require(size     >  0, s"Size not positive: $position")
    // If necessary, split lower entry.
    Option(entries.lowerEntry(position)).foreach { case JEntry(storedAt, area) =>
      val distance = position - storedAt
      if area._length > distance then
        val (head, tail) = area._split(distance)
        entries.put(storedAt, head)
        entries.put(storedAt + distance, tail)
    }

    // If necessary, drop or trim ceiling entries (including the higher part of the entry split before).
    while Option(entries.ceilingKey(position)).exists { storedAt =>
      val overlap = position + size - storedAt
      if overlap <= 0 then false else
        val area = entries.remove(storedAt)
        if (overlap < area._length) then
          entries.put(storedAt + overlap, area._drop(overlap))
          release(Seq(overlap))
          false
        else
          entries.remove(storedAt)
          release(Seq(area._length))
          true
    } do {/**/}

  /** Truncates the managed areas to the specified size. */
  def keep(newSize: Long): Unit =
    require(newSize >= 0, s"Negative new size: $newSize")
    // Remove higher entries.
    release(entries.tailMap(newSize).asScala.values.map(_._length))
    // Keep all strictly lower entries.
    entries = java.util.TreeMap(entries.headMap(newSize)) // The headMap view doesn't accept out-of-range keys.
    // If necessary, trim highest remaining entry.
    Option(entries.lastEntry()).foreach { case JEntry(storedAt, area) =>
      val distance = newSize - storedAt
      if (distance < area._length) entries.put(storedAt, area._keep(distance))
    }

  def mergeIfPossible(position: Long): Unit =
    require(position >= 0, s"Negative position: $position")
    Option(entries.lowerEntry(position)).foreach { case JEntry(storedAt, area) =>
      val entryLength = area._length
      require(storedAt + entryLength <= position, s"Overlapping entries at $position: $entries")
      if storedAt + entryLength == position then
        Option(entries.get(position)) match
          case None => throw IllegalArgumentException(s"No entry at position: $position")
          case Some(upperArea) =>
            area._merge(upperArea).foreach { merged =>
              entries.remove(position)
              entries.put(storedAt, merged)
            }
    }

  /** Reads available data chunks.
    *
    * @return An Iterator of (position, gapSize | byte array]). */
  def read(position: Long, size: Long): Iterator[(Long, Either[Long, M])] =
    // Note: For Scala 3.0, generic union types like Long | M seem to still be unwieldy.
    require(position >= 0, s"Negative position: $position")
    require(size     >  0, s"Size not positive: $position")
    // Identify the relevant entries.
    val startKey = Option(entries.floorKey(position)).getOrElse(position)
    import scala.jdk.CollectionConverters.MapHasAsScala
    var section = entries.subMap(startKey, position + size).asScala.toVector
    // Trim or remove the head entry if necessary.
    if section.nonEmpty && startKey < position then
      val (headPosition -> headData) +: tail = section
      val distance = position - headPosition
      if headData._length <= distance then section = tail
      else section = (position -> headData._drop(distance)) +: tail
    if section.isEmpty then Iterator(position -> Left(size)) else
      // Truncate the last entry if necessary.
      val lead :+ (tailPosition -> tailData) = section
      val keep = position + size - tailPosition
      if keep < tailData._length then section = lead :+ (tailPosition -> tailData._keep(keep))
      // Assemble result.
      def recurse(section: Vector[(Long, M)], currentPos: Long, remainingSize: Long): LazyList[(Long, Either[Long, M])] =
        if section.isEmpty then
          if remainingSize == 0 then LazyList() else LazyList(currentPos -> Left(remainingSize))
        else
          val (entryPos -> data) +: rest = section
          if entryPos == currentPos then
            (entryPos -> Right(data)) #:: recurse(rest, entryPos + data._length, remainingSize - data._length)
          else
            val distance = entryPos - currentPos
            (currentPos -> Left(distance)) #:: recurse(section, entryPos, remainingSize - distance)
      recurse(section, position, size).iterator
