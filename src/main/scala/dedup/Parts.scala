package dedup

// FIXME new
case class Parts(parts: Seq[(Long, Long)]) {
  require(parts.forall{ case (start, stop) => stop > start }, s"Illegal parts $parts")
  def isEmpty: Boolean = parts.isEmpty
  def size: Long = parts.map{ case (start, stop) => stop - start }.sum
  def drop(offset: Long): Parts =
    Parts(parts.foldLeft((Vector[(Long, Long)](), offset)){ case ((result, offset), (start, stop)) =>
      val length = stop - start
      (if (length <= offset) result else result :+ (start + offset, stop)) -> math.max(0, offset - length)
    }._1)
  def take(size: Long): Seq[(Long, Long)] =
    parts.foldLeft((Vector[(Long, Long)](), size)) { case ((result, remaining), (start, stop)) =>
      val length = math.min(stop - start, remaining)
      (if (remaining == 0) result else result :+ (start, stop + length)) -> (remaining - length)
    }._1
  def range(offset: Long, size: Long): Seq[(Long, Long)] =
    drop(offset).take(size)
}
