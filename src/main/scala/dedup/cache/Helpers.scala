package dedup
package cache

object JEntry:
  def unapply[K,V](e: java.util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue)

extension(long: Long)
  /** Ensures the Long value is within the range of 0 / positive Int before conversion. */
  def asInt: Int =
    ensure("cache.int", long >= 0 && long <= Int.MaxValue, s"Illegal int+ value: $long")
    long.toInt
