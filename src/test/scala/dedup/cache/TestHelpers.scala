package dedup
package cache

extension(l: Iterator[(Long, Either[Long, Array[Byte]])])
  def _seq = l.map { case (pos, Right(data)) => pos -> Right(data.toSeq); case other => other }.toSeq

val MaxInt: Long = Int.MaxValue
