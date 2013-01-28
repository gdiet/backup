// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.plugins.ConsoleProgressOutput
import net.diet_rich.util.vals._

trait BackupMonitor[SourceType <: TreeSource[SourceType]] {
  def notifyProgressMonitor(entry: SourceType): Unit
}

trait BackupThreadManager {
  def executeInThreadPool(f: => Unit): Unit
}

trait BackupErrorHandler[SourceType <: TreeSource[SourceType]] {
  def catchAndHandleException(entry: SourceType)(f: => Unit): Unit
}

trait BackupControl[SourceType <: TreeSource[SourceType]]
extends BackupMonitor[SourceType] with BackupThreadManager with BackupErrorHandler[SourceType] {
  def shutdown: Unit
}

trait MemoryManager {
  /** @return An array of the requested size or None. */
  def getLargeArray(size: Size): Option[Array[Byte]]
}

trait SimpleBackupControl extends BackupControl[FileSource] {
  def notifyProgressMonitor(entry: FileSource): Unit = Unit // println("processing %s" format entry.file)
  def executeInThreadPool(f: => Unit): Unit = f
  def catchAndHandleException(entry: FileSource)(f: => Unit): Unit = f
  def shutdown: Unit = Unit
}

class PooledBackupControl extends BackupControl[FileSource] {
  private lazy val progressOutput = new ConsoleProgressOutput(
    "backup: %s files in %s directories after %ss", 5000, 5000)
  def notifyProgressMonitor(entry: FileSource): Unit =
    if (entry.file.isDirectory()) progressOutput.incDirs else progressOutput.incFiles
  val pool = new java.util.concurrent.ThreadPoolExecutor(8, 8, 0,
    java.util.concurrent.TimeUnit.SECONDS,
    new java.util.concurrent.LinkedBlockingQueue[Runnable](4),
    new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy)
  val tasks = new java.util.concurrent.atomic.AtomicInteger(0)
  val tasksLock = new java.util.concurrent.Semaphore(1)
  def executeInThreadPool(f: => Unit): Unit = {
    if (tasks.incrementAndGet == 1) {
      if (!tasksLock.tryAcquire()) {
        tasks.decrementAndGet
        throw new IllegalStateException("could not aquire tasks lock")
      }
    }
    pool.execute(new Runnable { def run: Unit = try { f } finally {
      if (tasks.decrementAndGet == 0) tasksLock.release
    } })
  }
  def catchAndHandleException(entry: FileSource)(f: => Unit): Unit =
    try { f } catch { case e: Throwable => println(e) }
  def shutdown = {
    tasksLock.acquire
    if (!(tasks.get == 0)) throw new IllegalStateException("not all tasks have been released, count is ${tasks.get}")
    progressOutput.cancel
    pool.shutdown
    pool.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS)
  }
}

trait SimpleMemoryManager extends MemoryManager {
  /** @return An array of the requested size or None. */
  def getLargeArray(size: Size): Option[Array[Byte]] =
    if (size < Size(10000000)) Some(new Array[Byte](size.value toInt)) else None
}
