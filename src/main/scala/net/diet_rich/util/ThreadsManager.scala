// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

class ThreadsManager(numberOfThreads: Int, queueSize: Int = 4) {
  
  private val executor = new java.util.concurrent.ThreadPoolExecutor(
    numberOfThreads, numberOfThreads, 1, java.util.concurrent.TimeUnit.DAYS,
    new java.util.concurrent.LinkedBlockingQueue[Runnable](queueSize),
    new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
  )
  private val runningTasks = new java.util.concurrent.atomic.AtomicInteger(0)
  private val tasksLock = new java.util.concurrent.Semaphore(1)
  
  def execute(task: => Unit) = {
    if (runningTasks.incrementAndGet == 1) {
      if (!tasksLock.tryAcquire()) {
        runningTasks.decrementAndGet
        throw new IllegalStateException("could not aquire tasks lock")
      }
    }
    executor.execute(new Runnable { def run: Unit =
      try { task }
      finally { if (runningTasks.decrementAndGet == 0) tasksLock.release }
    })
  }
  
  def shutdown: Unit = {
    tasksLock.acquire
    if (!(runningTasks.get == 0)) throw new IllegalStateException("not all tasks have been released, count is ${tasks.get}")
    executor.shutdown
    executor.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS)
  }
}
