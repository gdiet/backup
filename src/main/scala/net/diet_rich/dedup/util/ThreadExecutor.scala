package net.diet_rich.dedup.util

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

trait ThreadExecutor {
  protected val executionThreads: Int
  private val executor = BlockingThreadPoolExecutor(executionThreads)
  private val executionContext = ExecutionContext fromExecutorService executor
  protected def execute[T] (f: => T): T = Await result (Future(f)(executionContext), 1 day)
  protected def shutdownExecutor() = executor.shutdownAndAwaitTermination()
}
