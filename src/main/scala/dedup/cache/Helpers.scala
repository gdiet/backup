package dedup
package cache

object JEntry:
  def unapply[K,V](e: java.util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue)

extension(long: Long) def asInt: Int =
  require(long >= 0 && long <= Int.MaxValue, s"Illegal int+ value: $long")
  long.toInt
