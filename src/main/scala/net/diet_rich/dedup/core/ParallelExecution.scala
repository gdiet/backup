package net.diet_rich.dedup.core

import net.diet_rich.dedup.util.{ThreadExecutors, systemCores}

import scala.collection.TraversableOnce
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, ExecutionContext}

trait ParallelExecution extends AutoCloseable {
  protected val parallel: Option[Int]

  private val executor = ThreadExecutors.threadPoolOrInlineExecutor(parallel getOrElse systemCores)
  protected implicit val executionContext = ExecutionContext fromExecutorService executor

  protected def awaitForever[T](future: Future[T]): T = Await.result(future, Duration.Inf)
  protected def combine(futures: List[Future[Unit]]): Future[Unit] = Future sequence futures map (_ => ())

  override def close() = executor close()
}
