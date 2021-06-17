package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class WriteCacheSpec extends AnyFreeSpec:

  s"${°[MemCache]} uses ${°[CacheBase[_]]}, see also there..." - {
    val available = AtomicLong(100)
    val cache = MemCache(available)
    "The write method" - {
      "called with data exceeding the available cache size" - {
        "returns false" in assert(cache.write(200, new Array[Byte](101)) == false)
        "doesn't change the cache contents" in assert(cache.read(0, 300) == Seq(0 -> Left(300)))
      }
      "called with data that can be added as first entry" - {
        "returns true" in assert(cache.write(20, Array[Byte](1,2)) == true)
        "updates the cache contents" in assert(cache.read(0, 50)._seq == Seq(0 -> Left(20), 20 -> Right(Seq[Byte](1,2)), 22 -> Left(28)))
        "updates available count" in assert(available.get == 98)
      }
      "called with data that overwrites an existing entry" - {
        "returns true" in assert(cache.write(19, Array[Byte](1,2,3,4)) == true)
        "updates the cache contents" in assert(cache.read(0, 50)._seq == Seq(0 -> Left(19), 19 -> Right(Seq[Byte](1,2,3,4)), 23 -> Left(27)))
        "updates available count" in assert(available.get == 96)
      }
      "called with data that overwrites the end of a data entry" - {
        "returns true" in assert(cache.write(22, Array[Byte](1,2)) == true)
        "updates the cache contents, merging entries" in assert(cache.read(0, 50)._seq == Seq(0 -> Left(19), 19 -> Right(Seq[Byte](1,2,3,1,2)), 24 -> Left(26)))
        "updates available count" in assert(available.get == 95)
      }
      "called with data that overwrites the start of a data entry" - {
        "returns true" in assert(cache.write(18, Array[Byte](1,2)) == true)
        "updates the cache contents, not merging entries" in assert(cache.read(0, 50)._seq == Seq(0 -> Left(18), 18 -> Right(Seq[Byte](1,2)), 20 -> Right(Seq[Byte](2,3,1,2)), 24 -> Left(26)))
        "updates available count" in assert(available.get == 94)
      }
      "called with data that overwrites the middle of a data entry" - {
        "returns true" in assert(cache.write(21, Array[Byte](8,9)) == true)
        "updates the cache contents, merging two entries" in assert(cache.read(0, 50)._seq == Seq(0 -> Left(18), 18 -> Right(Seq[Byte](1,2)), 20 -> Right(Seq[Byte](2,8,9)), 23 -> Right(Seq[Byte](2)), 24 -> Left(26)))
        "updates available count" in assert(available.get == 94)
      }
    }
  }

  s"${°[Allocation]} uses ${°[CacheBase[_]]}, see also there..." - {
    "needs a test for readData unless that method is never used" - {} // FIXME
  }

  °[CacheBase[_]] - {
    object cache extends CacheBase[Int]:
      override protected def length(m: Int): Long = m
      override protected def merge (m: Int, n       : Int ): Option[Int] = Some(m + n)
      override protected def drop  (m: Int, distance: Long): Int        = m - distance.asInt
      override protected def keep  (m: Int, distance: Long): Int        = distance.asInt
      override protected def split (m: Int, distance: Long): (Int, Int) = (distance.asInt, m - distance.asInt)
      private var partsDropped = Seq[Long]()
      def dropped = partsDropped.tap { _ => partsDropped = Seq() }
      override protected def release(sizes: => Iterable[Long]): Unit = partsDropped ++= sizes.toSeq
      import scala.jdk.CollectionConverters.MapHasAsScala
      def map = entries.asScala
      def set(values: (Long, Int)*) =
        entries.clear()
        values.foreach(kv => entries.put(kv._1, kv._2))

    val MaxInt: Long = Int.MaxValue

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
