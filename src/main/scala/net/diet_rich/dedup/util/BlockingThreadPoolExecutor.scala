package net.diet_rich.dedup.util

import java.util.concurrent._

object BlockingThreadPoolExecutor {
  trait GracefulShutdown { def shutdownAndAwaitTermination(): Unit }
  
  def apply(threadPoolSize: Int) = {
    val executorQueue = new ArrayBlockingQueue[Runnable](threadPoolSize)
    val rejectHandler = new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = executorQueue put r
    }
    new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.SECONDS, executorQueue, rejectHandler) with GracefulShutdown {
      override def shutdownAndAwaitTermination() = {
        shutdown()
        awaitTermination(1, TimeUnit.DAYS)
      }
    }
  }
}
