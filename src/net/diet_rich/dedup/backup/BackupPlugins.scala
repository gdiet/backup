// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.vals._

trait BackupMonitor[SourceType <: TreeSource[SourceType]] {
  protected def notifyProgressMonitor(entry: SourceType): Unit
}

trait BackupThreadManager {
  protected def executeInThreadPool(f: => Unit): Unit
}

trait BackupErrorHandler[SourceType <: TreeSource[SourceType]] {
  protected def catchAndHandleException(entry: SourceType)(f: => Unit): Unit
}

trait BackupControl[SourceType <: TreeSource[SourceType]]
extends BackupMonitor[SourceType] with BackupThreadManager with BackupErrorHandler[SourceType]

trait MemoryManager {
  /** @return An array of the requested size or None. */
  protected def getLargeArray(size: Size): Option[Array[Byte]]
}

trait SimpleBackupControl extends BackupControl[FileSource] {
  protected def notifyProgressMonitor(entry: FileSource): Unit = println("processing %s" format entry.file)
  protected def executeInThreadPool(f: => Unit): Unit = f
  protected def catchAndHandleException(entry: FileSource)(f: => Unit): Unit = f
}

trait SimpleMemoryManager extends MemoryManager {
  /** @return An array of the requested size or None. */
  protected def getLargeArray(size: Size): Option[Array[Byte]] =
    if (size < Size(10000000)) Some(new Array[Byte](size.value toInt)) else None
}
