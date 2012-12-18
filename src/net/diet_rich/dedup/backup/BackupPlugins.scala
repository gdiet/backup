// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

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
