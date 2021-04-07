package dedup.cache

import java.util.concurrent.atomic.AtomicLong

object MemCachePerformance extends App {
  val available = new AtomicLong(2000000000) // 2 GB, 500000 x 4000

  (1 to 5).foreach { _ =>
    System.gc()
    Thread.sleep(200)
    val start = System.nanoTime()
    val memCache = new MemCache()(new ByteArrayArea(available))
    var vector = Vector[Array[Byte]]()
    (1 to 500000).foreach { _ =>
      vector :+=  new Array[Byte](4000)
    }
    val end = System.nanoTime()
    println(s"Time: ${(end-start) / 1000} us")
  }

  println()

  (1 to 5).foreach { _ =>
    System.gc()
    Thread.sleep(200)
    val start = System.nanoTime()
    val memCache = new MemCache()(new ByteArrayArea(available))
    var vector = Vector[Array[Byte]]()
    (1 to 500000).foreach { n =>
      memCache.write(n * 4000, new Array[Byte](4000))
    }
    val end = System.nanoTime()
    println(s"Time: ${(end-start) / 1000} us")
  }

}
