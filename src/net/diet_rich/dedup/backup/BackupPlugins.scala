// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

trait BackupMonitor {
  type Source
  protected def notifyProgressMonitor(entry: Source): Unit
}

trait BackupThreadManager {
  protected def executeInThreadPool(f: => Unit): Unit
}

trait BackupErrorHandler {
  type Source
  protected def catchAndHandleException(entry: Source)(f: => Unit): Unit
}

trait BackupPluginDummy extends BackupMonitor with BackupThreadManager with BackupErrorHandler {
  protected def notifyProgressMonitor(entry: Source): Unit = Unit
  protected def executeInThreadPool(f: => Unit): Unit = f
  protected def catchAndHandleException(entry: Source)(f: => Unit): Unit = f
}
