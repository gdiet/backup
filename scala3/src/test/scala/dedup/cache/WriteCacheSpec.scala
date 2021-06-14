package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class WriteCacheSpec extends AnyFreeSpec:

  °[CacheBase[_]] - {
    object cache extends CacheBase[Int]:
      private var partsDropped = Seq[Long]()
      def dropped = partsDropped.tap { _ => partsDropped = Seq() }
      override protected def release(sizes: => Iterable[Long]): Unit = partsDropped ++= sizes.toSeq
      extension(m: Int)
        override protected def length: Long = m
        override protected def drop (distance: Long): Int = m - distance.asInt
        override protected def keep (distance: Long): Int = distance.asInt
        override protected def split(distance: Long): (Int, Int) = (distance.asInt, m - distance.asInt)
      import scala.jdk.CollectionConverters.MapHasAsScala
      def map = entries.asScala
      def set(values: (Long, Int)*) =
        entries.clear()
        values.foreach(kv => entries.put(kv._1, kv._2))

    val MaxInt: Long = Int.MaxValue

    "The keep method" - {
      "called with negative size throws an IllegalArgumentException" in {
        intercept[IllegalArgumentException] { cache.keep(-1) }
      }
      "does not change anything when called for a non-allocated area" in {
        cache.set(0L -> 100, 300L -> 100)
        cache.keep(400)
        assert(cache.map == Map(0L -> 100, 300L -> 100))
        assert(cache.dropped == Seq())
      }
      "drops all entries in the area to clear" in {
        cache.set(MaxInt -> 1000, MaxInt + 1000 -> 1000)
        cache.keep(MaxInt + 1000)
        assert(cache.map == Map(MaxInt -> 1000))
        assert(cache.dropped == Seq(1000))
      }
    }

    "The clear method" - {
      "called with negative position throws an IllegalArgumentException" in {
        intercept[IllegalArgumentException] { cache.clear(-1, 1) }
      }
      "called with zero or negative size throws an IllegalArgumentException" in {
        intercept[IllegalArgumentException] { cache.clear(1,  0) }
        intercept[IllegalArgumentException] { cache.clear(1, -1) }
      }
      "does not change anything when called for a non-allocated area" in {
        cache.set(0L -> 100, 300L -> 100)
        cache.clear(100, 200)
        assert(cache.map == Map(0L -> 100, 300L -> 100))
        assert(cache.dropped == Seq())
      }
      "drops all entries in the area to clear" in {
        cache.set(MaxInt - 1000 -> 1000, MaxInt -> 1000, MaxInt + 1000 -> 500, MaxInt + 1500 -> 500)
        cache.clear(MaxInt, 1500)
        assert(cache.map == Map(MaxInt - 1000 -> 1000, MaxInt + 1500 -> 500))
        assert(cache.dropped == Seq(1000, 500))
      }
      "cuts a hole into an entry if required" in {
        cache.set(MaxInt - 1000 -> 2000)
        cache.clear(MaxInt - 500, 1000)
        assert(cache.map == Map(MaxInt - 1000 -> 500, MaxInt + 500 -> 500))
        assert(cache.dropped == Seq(1000))
      }
      "trims end or start of entries if required" in {
        cache.set(MaxInt - 1000 -> 1000, MaxInt -> 1000)
        cache.clear(MaxInt - 500, 900)
        assert(cache.map == Map(MaxInt - 1000 -> 500, MaxInt + 400 -> 600))
        assert(cache.dropped == Seq(500, 400))
      }
    }
  }
