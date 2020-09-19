package dedup.store

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

object LongTermStoreTest extends App {
  test()

  implicit class Check[T](val actual: T) extends AnyVal {
    def is(expected: T): Unit = { println(". check"); require(actual == expected, s"$actual is not $expected") }
  }

  def delete(dir: java.io.File): Unit =
    java.nio.file.Files.walk(dir.toPath).sorted(java.util.Comparator.reverseOrder()).forEach(t => t.toFile.delete())

  def test(): Unit = {
    testPathOffsetSize()
    testParallelAccess()
    testLongTermStore("dedupfs-temp/LongTermStoreTest")
  }

  def testLongTermStore(basePath: String): Unit = {
    import java.io.File
    val dir = new File(basePath)
    if (dir.exists()) delete(dir)
    try {
      dir.mkdirs()
      val store = new LongTermStore(dir, false)
      println(". reading an area not covered by data files returns zeros (and ERROR/exception in log)")
      store.read(0, 10).toSeq.is(Seq.fill[Byte](10)(0))
      println(". reading some bytes previously written succeeds")
      store.write(0, Array[Byte](1,2,3,4))
      store.read(1, 2).toSeq.is(Seq[Byte](2,3))
      println(". reading beyond the data written returns zeros")
      store.read(2, 4).toSeq.is(Seq[Byte](3,4,0,0))
    } finally delete(dir)
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
    locally {
      val p = new P
      p.access("1", write = false)(identity)
      p.access("2", write = false)(identity)
      p.access("1", write = false)(identity)
      p.access("3", write = false)(identity)
      p.access("3", write = true)(identity)
      p.close()
      println(". resources are re-used or closed in LRU fashion when the limit is reached")
      p.actions.slice(0, 4).is(Vector("o1", "o2", "c2", "o3"))
      println(". a resource opened for reading is closed and re-opened for writing")
      p.actions.slice(4, 6).is(Vector("c3", "o3w"))
      println(". close closes all currently open resources")
      p.actions.slice(6, 99).is(Vector("c1", "c3"))
    }
    locally {
      val p = new P
      val p1 = Promise[Unit]()
      val p2 = Promise[Unit]()
      var r: Vector[String] = Vector()
      val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
      p.access("1", write = false)(_.tap{ _ =>
        Future{ p.access("0", write = true)(_.tap{ _ =>
          synchronized { r :+= s"0-${p1.isCompleted}" }
          Thread.sleep(40)
        })}(ec)
        Future{ p.access("1", write = true)(_.tap{ _ =>
          synchronized { r :+= s"2-${p1.isCompleted}" }
          p2.complete(Success(()))
        })}(ec)
        Future{ p.access("2", write = true)(_.tap{ _ =>
          synchronized { r :+= s"3-${p2.isCompleted}" }
        })}(ec)
        Thread.sleep(20)
        synchronized { r :+= s"1" }
        p1.complete(Success(()))
      })
      Thread.sleep(50)
      ec.shutdown()

      println(". access to a resource from a second thread is blocked while the resource is accessed")
      r.slice(1, 3).is(Vector("1", "2-true"))
      println(". access to a different resource from a second thread is possible while a resource is accessed")
      r.slice(0, 2).is(Vector("0-false", "1"))
      println(". no more resources than configured can be accessed in parallel")
      r.slice(2, 99).is(Vector("2-true", "3-true"))
    }
  }

  def testPathOffsetSize(): Unit = {
    import LongTermStore._
    println("Tests for LongTermStore")
    println(". file size should be 100.000.000")
    fileSize.is(100000000)
    println(". a location in the first data file that would exceed the file's size")
    pathOffsetSize(        20000000, 100000000).is((      "00/00/0000000000", 20000000, 80000000))
    println(". a location in a data in the first folder")
    pathOffsetSize(       120000000,         1).is((      "00/00/0100000000", 20000000,        1))
    println(". a location in a data in a folder beyond the normal limit")
    pathOffsetSize(112340120000000L,         1).is(("112/34/112340100000000", 20000000,        1))
  }
}
