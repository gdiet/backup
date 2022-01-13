package dedup
package server

class FreeAreasSpec extends org.scalatest.freespec.AnyFreeSpec {
  private class _FreeAreas extends FreeAreas:
    def _free: Seq[Chunk] = free

  "For FreeAreas objects, certain requirements are checked to avoid programming mistakes:" - {
    val free = _FreeAreas().tap(_.set(Seq(Chunk(10, Long.MaxValue))))
    "Get fails for size zero"     in intercept[IllegalArgumentException](free.get( 0))
    "Get fails for negative size" in intercept[IllegalArgumentException](free.get(-1))
    "Set fails if the last chunk doesn't end at MAXLONG" in
      intercept[IllegalArgumentException](free.set(Seq(Chunk(10,Long.MaxValue - 1))))
  }

  "A FreeAreas object containing a single chunk" - {
    val free = _FreeAreas().tap(_.set(Seq(Chunk(10, Long.MaxValue))))
    "should return a single chunk for a get" in assert(free.get(1000000000) == Seq(Chunk(10, 1000000010)))
    "should have the free size reduced afterwards" in assert(free._free == Seq(Chunk(1000000010, Long.MaxValue)))
  }

  "When using a FreeAreas object containing three chunks" - {
    def freeFactory() = _FreeAreas().tap(_.set(Seq(
      Chunk(10, 20),
      Chunk(100,200),
      Chunk(1000, Long.MaxValue)
    )))

    "to request exactly the first chunk" - {
      val free = freeFactory()
      "get should return one chunk" in assert(
        free.get(10) == Seq(Chunk(10, 20))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(Chunk(100, 200), Chunk(1000, Long.MaxValue))
      )
    }

    "to request more than the first and less than the second chunk" - {
      val free = freeFactory()
      "get should return two chunks" in assert(
        free.get(30) == Seq(Chunk(10, 20), Chunk(100, 120))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(Chunk(120, 200), Chunk(1000, Long.MaxValue))
      )
    }

    "to request more than the second chunk" - {
      val free = freeFactory()
      "get should return three chunks" in assert(
        free.get(130) == Seq(Chunk(10, 20), Chunk(100, 200), Chunk(1000, 1020))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(Chunk(1020, Long.MaxValue))
      )
    }
  }
}
