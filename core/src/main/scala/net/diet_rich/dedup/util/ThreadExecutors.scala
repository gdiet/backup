package net.diet_rich.dedup.util

import java.util.concurrent._

object ThreadExecutors {
  private def threadPoolExecutor(parallel: Int)(rejectHandler: (BlockingQueue[Runnable], Runnable) => Unit) = {
    val executorQueue = new ArrayBlockingQueue[Runnable](parallel)
    val rejectedExecutionHandler = new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = rejectHandler(executorQueue, r)
    }
    new ThreadPoolExecutor(parallel, parallel, 0, TimeUnit.SECONDS, executorQueue, rejectedExecutionHandler) with AutoCloseable {
      override def close() = {
        shutdown()
        awaitTermination(1, TimeUnit.DAYS)
      }
    }
  }
  def blockingThreadPoolExecutor(parallel: Int) = threadPoolExecutor(parallel) {
    case (executorQueue, runnable) => executorQueue put runnable
  }
  def threadPoolOrInlineExecutor(parallel: Int) = threadPoolExecutor(parallel) {
    case (_, runnable) => runnable run()
  }
}
