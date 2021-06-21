package dedup
package cache

import org.scalatest._
import org.scalatest.freespec._

import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class WriteCacheSpec extends AnyFreeSpec with TestFile:
  val available = new AtomicLong(10)
  val cache = WriteCache(available, testFile.toPath, 5)

  "Reading the initial cache yields a hole of the initial size" in {
    assert(cache.read(0, 100)._seq == Seq(0 -> Left(5)))
  }
  "Writing beyond end of cache is possible" in {
    cache.write(7, Array[Byte](1, 2))
    assert(cache.read(0, 100)._seq == Seq(
      0 -> Left(5),
      5 -> Right(Seq(0, 0)),
      7 -> Right(Seq(1, 2))
    ))
  }
