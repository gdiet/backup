package net.diet_rich.fdfs

import java.sql.Connection
import java.util.concurrent.TimeUnit

// TODO for backup, set up delayed insert db based on configuration
// backup.delayedInsert = true|false
// backup.delayedInsert.autocommit = true|false
// backup.delayedInsert.batchsize = <Int, 0: no batch>
// backup.delayedInsert.threads = <Int, 0: inline>

class DeferredInsertTreeDB(
      connection: Connection,
      // FIXME not yet used
      commit: Boolean,
      // FIXME not yet used
      batchSize: Int,
      queueSize: Int,
      threads: Int
    ) extends TreeSqlDB(connection) {

  protected val executor =
    if (threads <= 0)
      // inline execution
      // FIXME not yet functional
      new java.util.concurrent.Executor() {
        def execute(command: Runnable) = command
        def shutdown() : Unit = Unit
        def awaitTermination(timeout: Long, unit: TimeUnit) : Boolean = true
      }
    else
      // deferred execution
      new java.util.concurrent.ThreadPoolExecutor(
        threads, threads, 1, TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue[Runnable](queueSize),
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
      )
  
  def shutdown = {
    executor.shutdown
    executor.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
  }
  
  implicit def closureToRunnable(closure: => Any) : Runnable = {
    new Runnable() { def run: Unit = closure }
  }
  
  override def create(parent: Long, name: String, time: Long, data: Long) : Long = {
    val id = maxEntryId incrementAndGet()
    // FIXME check WHEN the closure is evaluated
    executor.execute { addEntry(id, parent, name, time, data) }
    id
  }
  
  
}