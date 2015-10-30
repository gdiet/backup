package net.diet_rich.common

import java.util.concurrent._
import scala.concurrent.ExecutionContext

object Executors {
  private def threadPoolExecutor(poolSize: Int)(rejectHandler: (BlockingQueue[Runnable], Runnable) => Unit) = {
    val executorQueue = new ArrayBlockingQueue[Runnable](poolSize)
    val rejectedExecutionHandler = new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor): Unit = rejectHandler(executorQueue, r)
    }
    new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.SECONDS, executorQueue, rejectedExecutionHandler) with AutoCloseable {
      override def close() = new RichExecutorService(this).close()
    }
  }

  def blockingThreadPoolExecutor(poolSize: Int) = threadPoolExecutor(poolSize) {
    case (executorQueue, runnable) => executorQueue put runnable
  }
  def threadPoolOrInlineExecutor(poolSize: Int) = threadPoolExecutor(poolSize) {
    case (_, runnable) => runnable run()
  }

  def blockingThreadPoolExecutionContext(poolSize: Int) = ExecutionContext fromExecutorService blockingThreadPoolExecutor(poolSize)
  def threadPoolOrInlineExecutionContext(poolSize: Int) = ExecutionContext fromExecutorService threadPoolOrInlineExecutor(poolSize)

  implicit class RichExecutorService(val service: ExecutorService) extends AnyVal {
    def close() = {
      service shutdown()
      service awaitTermination(1, TimeUnit.DAYS)
    }
  }
}
