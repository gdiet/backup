// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.util.Size

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

trait BackupControlDummy[SourceType <: TreeSource[SourceType]] extends BackupControl[SourceType] {
  protected def notifyProgressMonitor(entry: SourceType): Unit = Unit
  protected def executeInThreadPool(f: => Unit): Unit = f
  protected def catchAndHandleException(entry: SourceType)(f: => Unit): Unit = f
}

trait MemoryManager {
  /** @return An array of the requested size or None. */
  protected def getLargeArray(size: Size): Option[Array[Byte]]
}

trait MemoryManagerDummy {
  /** @return An array of the requested size or None. */
  protected def getLargeArray(size: Size): Option[Array[Byte]] =
    if (size < Size(1000000)) Some(new Array[Byte](size toInt)) else None
}
