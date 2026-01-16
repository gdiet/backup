package dedup
package cache

import java.util.concurrent.atomic.AtomicLong

// No IDEA support for scalatest with scala 3? https://youtrack.jetbrains.com/issue/SCL-18644
class AllocationSpec extends org.scalatest.freespec.AnyFreeSpec:

  s"${°[Allocation]} uses ${°[CacheBase[_]]}, see also there..." - {
    val cache = Allocation()
    "example 1 reading more than memChunk bytes" in {
      cache.allocate(100_000, 10)
      cache.allocate(100_010, memChunk)
      assert(cache.readData(99_000, 20_000 + memChunk)._seq == Seq(
        99_000 -> Left(1000),
        100_000 -> Right(Seq.fill[Byte](memChunk)(0)),
        100_000 + memChunk -> Right(Seq.fill[Byte](10)(0)),
        100_010 + memChunk -> Left(20_000 - 1000 - 10)
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
