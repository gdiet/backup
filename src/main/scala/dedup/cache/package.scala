package dedup

import java.util

package object cache {
  object Entry { def unapply[K,V](e: util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue) }

  implicit class LongDecorator(val long: Long) extends AnyVal {
    def asInt: Int = { assert(long >= 0 && long <= Int.MaxValue, s"Illegal value $long"); long.toInt }
  }
}
