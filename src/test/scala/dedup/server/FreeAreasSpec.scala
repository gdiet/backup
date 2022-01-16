package dedup
package server

class FreeAreasSpec extends org.scalatest.freespec.AnyFreeSpec {
  private class _FreeAreas(initialFree: Seq[DataArea]) extends FreeAreas:
    free = initialFree
    def _free: Seq[DataArea] = free

  "For FreeAreas objects, certain requirements are checked to avoid programming mistakes:" - {
    val free = FreeAreas(Seq(DataArea(10, Long.MaxValue)))
    "Get fails for size zero"     in intercept[IllegalArgumentException](free.reserve( 0))
    "Get fails for negative size" in intercept[IllegalArgumentException](free.reserve(-1))
    "Create fails if the last chunk doesn't end at MAXLONG" in
      intercept[IllegalArgumentException](FreeAreas(Seq(DataArea(10,Long.MaxValue - 1))))
  }

  "A FreeAreas object containing a single chunk" - {
    val free = _FreeAreas(Seq(DataArea(10, Long.MaxValue)))
    "should return a single chunk for a get" in assert(free.reserve(1000000000) == Seq(DataArea(10, 1000000010)))
    "should have the free size reduced afterwards" in assert(free._free == Seq(DataArea(1000000010, Long.MaxValue)))
  }

  "When using a FreeAreas object containing three chunks" - {
    def freeFactory() = _FreeAreas(Seq(
      DataArea(10, 20),
      DataArea(100,200),
      DataArea(1000, Long.MaxValue)
    ))

    "to request exactly the first chunk" - {
      val free = freeFactory()
      "get should return one chunk" in assert(
        free.reserve(10) == Seq(DataArea(10, 20))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(DataArea(100, 200), DataArea(1000, Long.MaxValue))
      )
    }

    "to request more than the first and less than the second chunk" - {
      val free = freeFactory()
      "get should return two chunks" in assert(
        free.reserve(30) == Seq(DataArea(10, 20), DataArea(100, 120))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(DataArea(120, 200), DataArea(1000, Long.MaxValue))
      )
    }

    "to request more than the second chunk" - {
      val free = freeFactory()
      "get should return three chunks" in assert(
        free.reserve(130) == Seq(DataArea(10, 20), DataArea(100, 200), DataArea(1000, 1020))
      )
      "the free size should be reduced afterwards" in assert(
        free._free == Seq(DataArea(1020, Long.MaxValue))
      )
    }
  }
}
