package dedup.store

object LongTermStoreTest extends App {
  test()

  implicit class Check[T](val actual: T) extends AnyVal {
    def is(expected: T): Unit = { println(". check"); require(actual == expected, s"$actual is not $expected") }
  }

  def test(): Unit = {
    import LongTermStore._
    println("Tests for LongTermStore")
    fileSize.is(100000000)
    pathOffsetSize(        20000000, 100000000).is((      "00/00/0000000000", 20000000, 80000000))
    pathOffsetSize(       120000000,         1).is((      "00/00/0100000000", 20000000,        1))
    pathOffsetSize(112340120000000L,         1).is(("112/34/112340100000000", 20000000,        1))
  }
}
