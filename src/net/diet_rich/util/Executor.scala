// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait Executor {
  final def apply(command: => Unit): Unit = execute(command)
  def execute(command: => Unit): Unit
  def shutdownAndAwaitTermination: Unit
}

object Executor {
  def apply(threads: Int, queueSize: Int): Executor = {
    if (threads <= 0)
      // inline execution
      new Executor{
        override def execute(command: => Unit): Unit = command
        override def shutdownAndAwaitTermination: Unit = Unit
      }
    else
      // deferred execution
      new Executor{
        val executor = new java.util.concurrent.ThreadPoolExecutor(
            threads, threads, 1, java.util.concurrent.TimeUnit.SECONDS,
            new java.util.concurrent.LinkedBlockingQueue[Runnable](queueSize),
            new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
        )
        override def execute(command: => Unit): Unit = executor.execute(new Runnable {
          override def run(): Unit = command
        })
        override def shutdownAndAwaitTermination: Unit = {
          executor.shutdown
          executor.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS)
        }
      }
  }
}
