// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.plugins.ConsoleProgressOutput
import net.diet_rich.util._
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

class SimpleBackupControl extends BackupControl[FileSource] {
  def notifyProgressMonitor(entry: FileSource): Unit = Unit // println("processing %s" format entry.file)
  def executeInThreadPool(f: => Unit): Unit = f
  def catchAndHandleException(entry: FileSource)(f: => Unit): Unit = f
  def shutdown: Unit = Unit
}

class PooledBackupControl(con: Console, progressOutput: ConsoleProgressOutput) extends BackupControl[FileSource] {
  private val executor = new ThreadsManager(10, 10)
  def notifyProgressMonitor(entry: FileSource): Unit =
    if (entry.file.isDirectory()) progressOutput.incDirs else progressOutput.incFiles
  def executeInThreadPool(f: => Unit): Unit = executor.execute(f)
  def catchAndHandleException(entry: FileSource)(f: => Unit): Unit =
    try { f } catch { case e: Throwable =>
      con.println(e.toString)
      con.println(e.getStackTraceString)
    }
  def shutdown = {
    executor.shutdown
    progressOutput.close
  }
}

object SimpleMemoryManager extends MemoryManager {
  /** @return An array of the requested size or None. */
  def getLargeArray(size: Size): Option[Array[Byte]] =
    if (size < Size(10000000)) Some(new Array[Byte](size.value toInt)) else None
}
