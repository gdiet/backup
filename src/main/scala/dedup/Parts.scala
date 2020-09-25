package dedup

case class Parts(parts: Seq[Chunk]) {
  require(parts.forall(_.size > 0), s"Illegal parts $parts")
  def isEmpty: Boolean = parts.isEmpty
  def size: Long = combinedSize(parts)
  def drop(offset: Long): Parts =
    Parts(parts.foldLeft((Vector.empty[Chunk], offset)){ case ((result, offset), (start, stop)) =>
      val length = stop - start
      (if (length <= offset) result else result :+ (start + offset, stop)) -> math.max(0, offset - length)
    }._1)
  def take(size: Long): Seq[Chunk] =
    parts.foldLeft((Vector.empty[Chunk], size)) { case ((result, remaining), (start, stop)) =>
      val length = math.min(stop - start, remaining)
      (if (remaining == 0) result else result :+ (start, start + length)) -> (remaining - length)
    }._1
  def range(offset: Long, size: Long): Seq[Chunk] =
    drop(offset).take(size)
}
