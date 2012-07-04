package net.diet_rich.fdfs

import java.sql.Connection
import java.util.concurrent.TimeUnit

class DeferredInsertTreeDB(
      connection: Connection,
      executor: SqlDBCommon.Executor
    ) extends TreeSqlDB(connection) {

  // eventually, we could think about SQL batch execution
  // and turning autocommit off to get a few percent higher performance
  
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