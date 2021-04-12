package dedup.cache

import dedup.ClassLogging

object ClassLoggingPerformance extends App {
  class T extends ClassLogging {
    val x: (=> String) => Unit = log.info _
  }

  (1 to 10).foreach { _ =>
    val start = System.nanoTime()
    (1 to 1000000).foreach(_ => new T)
    val end = System.nanoTime()
    println((end - start)/1000000)
  }
}
