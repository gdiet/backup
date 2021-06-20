package dedup
package cache

extension(l: LazyList[(Long, Either[Long, Array[Byte]])])
  def _seq = l.map { case (pos, Right(data)) => pos -> Right(data.toSeq); case other => other }

val MaxInt: Long = Int.MaxValue
