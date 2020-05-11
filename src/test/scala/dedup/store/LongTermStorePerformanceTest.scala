package dedup.store

import System.{nanoTime => now}

object LongTermStorePerformanceTest extends App {
  test()

  implicit class Check[T](val actual: T) extends AnyVal {
    def is(expected: T): Unit = { println(". check"); require(actual == expected, s"$actual is not $expected") }
  }

  def delete(dir: java.io.File): Unit =
    java.nio.file.Files.walk(dir.toPath).sorted(java.util.Comparator.reverseOrder()).forEach(t => t.toFile.delete())

  def test(): Unit = {
    testLongTermStore("dedupfs-temp/LongTermStorePerformanceTest")
  }

  def testLongTermStore(basePath: String): Unit = {
    import java.io.File
    val dir = new File(basePath)
    if (dir.exists()) delete(dir)
    dir.mkdirs()
    def testRun(parallel: Int): Unit = {
      val store = new LongTermStore(dir.getAbsolutePath, false) {
        override val parallelOpenResources: Int = parallel
      }
      store.write(0, new Array[Byte](310000000))
      def testLoop(): Unit = {
        for(_ <- 1 to 20) {
          for(k <- 0 to 6) {
            for (b <- 0 to 3) {
              store.read(b*100000000 + k*1000, 1000)
            }
          }
        }
      }
      testLoop()
      val start = now
      testLoop()
      println(f"parallelOpenResources $parallel - time ${(now - start)/1000} us")
    }
    println("Multiple times read 1000 bytes from 4 different data files...")
    println("When data files are kept open between reads:")
    for (n <- 1 to 10) testRun(5)
    println("When each read requires to close & open a data file:")
    for (n <- 1 to 10) testRun(2)
  }
}
