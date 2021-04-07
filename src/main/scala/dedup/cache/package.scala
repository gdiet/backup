package dedup

import java.util

package object cache {
  object Entry { def unapply[K,V](e: util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue) }

  implicit class LongDecorator(val long: Long) extends AnyVal {
    def asInt: Int = { assert(long >= 0 && long <= Int.MaxValue, s"Illegal value $long"); long.toInt }
  }

  trait MemArea[M] {
    def length(t: M): Long
    def drop(t: M, distance: Long): M
    def dropRight(t: M, distance: Long): M
  }

  implicit class MemAreaOps[M](val m: M) extends AnyVal {
    def length(implicit memArea: MemArea[M]): Long = memArea.length(m)
    def drop(distance: Long)(implicit memArea: MemArea[M]): M = memArea.drop(m, distance)
    def dropRight(distance: Long)(implicit memArea: MemArea[M]): M = memArea.dropRight(m, distance)
  }

  implicit object ByteArrayArea extends MemArea[Array[Byte]] {
    override def length(t: Array[Byte]): Long = t.length
    override def drop(t: Array[Byte], distance: Long): Array[Byte] = t.drop(distance.asInt)
    override def dropRight(t: Array[Byte], distance: Long): Array[Byte] = t.dropRight(distance.asInt)
  }

  implicit object LongArea extends MemArea[Long] {
    override def length(t: Long): Long = t
    override def drop(t: Long, distance: Long): Long = t - distance
    override def dropRight(t: Long, distance: Long): Long = t - distance
  }

  implicit object IntArea extends MemArea[Int] {
    override def length(t: Int): Long = t
    override def drop(t: Int, distance: Long): Int = t - distance.asInt
    override def dropRight(t: Int, distance: Long): Int = t - distance.asInt
  }

  object MemAreaSection {
    def apply[M: MemArea](entries: util.NavigableMap[Long, M], position: Long, size: Long): LazyList[Either[(Long, Long), (Long, M)]] = {
      // Identify the relevant entries.
      var section = Vector[(Long, M)]()
      val startKey = Option(entries.floorKey(position)).getOrElse(position)
      val subMap = entries.subMap(startKey, position + size - 1)
      subMap.forEach((pos: Long, dat: M) => section :+= (pos -> dat))
      if (section.isEmpty) LazyList(Left(position -> size)) else {

        // Trim or remove the head entry if necessary.
        if (startKey < position) {
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
          val (endPos, result) = section.foldLeft(0L -> LazyList[Either[(Long, Long), (Long, M)]]()) {
            case (position -> result, pos -> dat) =>
              if (position == pos) (position + dat.length) -> (result :+ Right(pos -> dat))
              else (pos + dat.length) -> (result :+ Left(position -> (pos - position)) :+ Right(pos -> dat))
          }
          if (distance >= 0) result else result :+ Left(endPos -> -distance)
        }
      }
    }
  }
}
