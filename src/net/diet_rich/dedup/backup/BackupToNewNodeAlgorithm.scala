// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup
import dedup.database.TreeEntryID
import dedup.database.BackupFileSystem

trait BackupToNewNodeAlgorithm[SourceType] {
  self : BackupMonitor with BackupThreadManager with BackupErrorHandler =>
  type Source = TreeSource[SourceType]

  def fs: BackupFileSystem
  
  def backup(source: Source, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    processSourceEntry(source, parent, reference)

  private def processSourceEntry(source: Source, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    catchAndHandleException(source) {
      monitorEntriesProcessed(source)
      // FIXME continue
    }
    
}


object BackupToNewNodeAlgorithmTryout {
  val backup = new BackupToNewNodeAlgorithm[FileSource] with BackupPluginDummy {
    def fs: BackupFileSystem = dedup.database.StubbedFileSystem
  }
  
  backup.backup(new FileSource(new java.io.File("")), TreeEntryID(0), None)
}