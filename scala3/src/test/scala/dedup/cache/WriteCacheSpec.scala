package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class WriteCacheSpec extends AnyFreeSpec with TestFile:
  val available = AtomicLong(10)
  val cache = WriteCache(available, testFile.toPath, 5)

  "Reading the initial cache yields a hole of the initial size" in {
    assert(cache.read(0, 100)._seq == Seq(0 -> Left(5)))
    assert(cache.size == 5)
  }
  "Writing beyond end of cache is possible" in {
    cache.write(7, Array[Byte](1, 2))
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(5),
      5 -> Right(Seq(0, 0)),
      7 -> Right(Seq(1, 2))
    ))
    assert(cache.size == 9)
    assert(available.get == 8)
  }
  val data12 = Array.fill[Byte](12)(3)
  "Scenario: Overwrite so file cache is used" in {
    cache.write(2, data12)
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(2),
      2 -> Right(data12.toSeq)
    ))
    assert(cache.size == 14)
    assert(available.get == 10)
  }
  "Scenario: Truncate extends cache" in {
    cache.truncate(20)
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(2),
      2 -> Right(data12.toSeq),
      14 -> Right(Seq(0, 0, 0, 0, 0, 0))
    ))
    assert(cache.size == 20)
    assert(available.get == 10)
  }
  "Scenario: Overwrite to memCache" in {
    cache.write(6, Array[Byte](6, 5))
    cache.write(12, Array[Byte](8, 7, 6, 5))
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(2),
      2 -> Right(Seq(3, 3, 3, 3)),
      6 -> Right(Seq(6, 5)),
      8 -> Right(Seq(3, 3, 3, 3)),
      12 -> Right(Seq(8, 7, 6, 5)),
      16 -> Right(Seq(0, 0, 0, 0))
    ))
    assert(cache.size == 20)
    assert(available.get == 4)
  }
  "Scenario: Truncate to shorten cache" in {
    cache.truncate(10)
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(2),
      2 -> Right(Seq(3, 3, 3, 3)),
      6 -> Right(Seq(6, 5)),
      8 -> Right(Seq(3, 3))
    ))
    assert(cache.size == 10)
    assert(available.get == 8)
  }
  "Closing the cache releases memory and deletes the temp file" in {
    cache.close()
    assert(available.get == 10)
    assert(testFile.exists() == false)
  }
