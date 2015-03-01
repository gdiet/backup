package net.diet_rich.dedup.util

import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

class ThreadExecutor(executionThreads: Int) extends AutoCloseable {
  private val executor = BlockingThreadPoolExecutor(executionThreads)
  private val executionContext = ExecutionContext fromExecutorService executor
  def apply[T] (f: => T): T = Await result (Future(f)(executionContext), 1 day)
  override def close() = executor.shutdownAndAwaitTermination()
}
