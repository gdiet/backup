package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class MemCacheSpec extends AnyFreeSpec:

  s"${°[MemCache]} uses ${°[CacheBase[_]]}, see also there..." - {
    val available = AtomicLong(100)
    val cache = MemCache(available)
    "The write method" - {
      "called with data exceeding the available cache size" - {
        "returns false" in assert(cache.write(200, new Array[Byte](101)) == false)
        "doesn't change the cache contents" in assert(cache.read(0, 300)._seq == Seq(0 -> Left(300)))
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
