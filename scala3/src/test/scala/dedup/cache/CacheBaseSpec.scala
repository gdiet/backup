package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class CacheBaseSpec extends AnyFreeSpec:

  °[CacheBase[_]] - {
    object cache extends LongCache:
      override protected def length(m: Long): Long = m
      override protected def merge (m: Long, n       : Long): Option[Long] = Some(m + n)
      override protected def drop  (m: Long, distance: Long): Long         = m - distance
      override protected def keep  (m: Long, distance: Long): Long         = distance
      override protected def split (m: Long, distance: Long): (Long, Long) = (distance, m - distance)
      private var partsDropped = Seq[Long]()
      def dropped = partsDropped.tap { _ => partsDropped = Seq() }
      override protected def release(sizes: => Iterable[Long]): Unit = partsDropped ++= sizes.toSeq
      import scala.jdk.CollectionConverters.MapHasAsScala
      def map = entries.asScala
      def set(values: (Long, Int)*) =
        entries.clear()
        values.foreach(kv => entries.put(kv._1, kv._2))

    "The read method" - {
      "called with negative position throws an IllegalArgumentException" in {
        intercept[IllegalArgumentException] { cache.read(-1, 1) }
      }
      "called with zero or negative size throws an IllegalArgumentException" in {
        intercept[IllegalArgumentException] { cache.read(1,  0) }
        intercept[IllegalArgumentException] { cache.read(1, -1) }
      }
      "returns no data when a hole is requested" in {
        cache.set(MaxInt - 1000 -> 500, MaxInt + 500 -> 500)
        val result = cache.read(MaxInt - 500, 1000)
        assert (result == Seq(MaxInt - 500 -> Left(1000)))
      }
      "returns a single byte if that is in cache" in {
        cache.set(0L -> 1)
        val result = cache.read(0, 1)
        assert (result == Seq(0 -> Right(1)))
      }
      "returns a trimmed start entry if requested" in {
        cache.set(1000L -> 1000)
        val result = cache.read(1600, 1000)
        assert (result == Seq(1600 -> Right(400), 2000 -> Left(600)))
      }
      "returns a trimmed end entry if requested" in {
        cache.set(1000L -> 1000)
        val result = cache.read(600, 1000)
        assert (result == Seq(600 -> Left(400), 1000 -> Right(600)))
      }
      "returns a full entry and holes to the left and right if requested" in {
        cache.set(1000L -> 1000)
        val result = cache.read(600, 2000)
        assert (result == Seq(600 -> Left(400), 1000 -> Right(1000), 2000 -> Left(600)))
      }
    }

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
