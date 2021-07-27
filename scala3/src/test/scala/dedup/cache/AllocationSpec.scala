package dedup
package cache

import java.util.concurrent.atomic.AtomicLong

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class AllocationSpec extends org.scalatest.freespec.AnyFreeSpec:

  s"${°[Allocation]} uses ${°[CacheBase[_]]}, see also there..." - {
    val cache = Allocation()
    "example 1 reading more than memChunk bytes" in {
      cache.allocate(100000, 10)
      cache.allocate(100010, memChunk)
      assert(cache.readData(99000, 20000 + memChunk)._seq == Seq(
        99000 -> Left(1000),
        100000 -> Right(Seq.fill[Byte](memChunk)(0)),
        100000 + memChunk -> Right(Seq.fill[Byte](10)(0)),
        100010 + memChunk -> Left(20000 - 1000 - 10)
      ))
    }
    "example 2 checking allocations larger than MaxInt" in {
      cache.clear(0, Long.MaxValue)
      cache.allocate(MaxInt + 10, MaxInt + 20)
      cache.allocate(2*MaxInt + 30, MaxInt)
      assert(cache.read(0, 4*MaxInt).toSeq == Seq(
        0 -> Left(MaxInt + 10),
        MaxInt + 10 -> Right(2*MaxInt + 20),
        3*MaxInt + 30 -> Left(MaxInt - 30)
      ))
    }
  }
