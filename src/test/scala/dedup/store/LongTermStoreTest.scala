package dedup.store

object LongTermStoreTest extends App {
  test()

  implicit class Check[T](val actual: T) extends AnyVal {
    def is(expected: T): Unit = { println(". check"); require(actual == expected, s"$actual is not $expected") }
  }

  def test(): Unit = {
    // FIXME re-enable
//    testPathOffsetSize()
    testParallelAccess()
  }

  def testParallelAccess(): Unit = {
    class P extends ParallelAccess[String] {
      var actions: Vector[String] = Vector.empty
      override protected val parallelOpenResources: Int = 2
      private def w(forWrite: Boolean) = if (forWrite) "w" else ""
      override protected def openResource(path: String, forWrite: Boolean): String =
        synchronized { actions :+= s"o$path${w(forWrite)}"; path }
      override protected def closeResource(path: String, r: String): Unit =
        synchronized { require(r == path, s"$r = $path"); actions :+= s"c$path" }
    }
    println("Tests for ParallelAccess")
    val p = new P
    p.access("1", write = false)(identity)
    p.access("2", write = false)(identity)
    p.access("1", write = false)(identity)
    p.access("3", write = false)(identity)
    p.access("3", write = true)(identity)
    p.close()
    println(p.actions)
    println(". resources are re-used or closed when the limit is reached")
    p.actions.slice(0, 4).is(Vector("o1", "o2", "c1", "o3"))
    println(". a resource opened for reading is closed and re-opened for writing")
    p.actions.slice(4, 6).is(Vector("c3", "o3w"))
    println(". close closes all currently open resources")
    p.actions.slice(6, 99).is(Vector("c2", "c3"))

//    val e1 = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
//    Future {}(e1)
  }

  def testPathOffsetSize(): Unit = {
    import LongTermStore._
    println("Tests for LongTermStore")
    fileSize.is(100000000)
    pathOffsetSize(        20000000, 100000000).is((      "00/00/0000000000", 20000000, 80000000))
    pathOffsetSize(       120000000,         1).is((      "00/00/0100000000", 20000000,        1))
    pathOffsetSize(112340120000000L,         1).is(("112/34/112340100000000", 20000000,        1))
  }
}
