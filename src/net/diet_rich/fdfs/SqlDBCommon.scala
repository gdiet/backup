package net.diet_rich.fdfs

import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.sql._
import java.util.concurrent.TimeUnit

protected trait SqlDBCommon {
  protected def readAsAtomicLong(statement: String)(implicit connection: Connection): AtomicLong =
    new AtomicLong(execQuery(connection, statement)(_ long 1) headOnly)
}

object SqlDBCommon {
  
  type Executor = {
    def execute(command: Runnable) : Unit
    def shutdownAndAwaitTermination : Unit
  }
  
  def executor(threads: Int, queueSize: Int) : Executor = {
    if (threads <= 0)
      // inline execution
      new java.util.concurrent.Executor() {
        override def execute(command: Runnable) : Unit = command.run
        def shutdownAndAwaitTermination : Unit = Unit
      }
    else
      // deferred execution
      new java.util.concurrent.ThreadPoolExecutor(
        threads, threads, 1, TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue[Runnable](queueSize),
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
      ) {
        def shutdownAndAwaitTermination : Unit = {
          shutdown
          awaitTermination(1, TimeUnit.DAYS)
        }
      }
  }
  
}