// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup
import dedup.database.TreeEntryID
import dedup.database.BackupFileSystem

trait BackupToNewNodeAlgorithm[SourceType <: TreeSource[SourceType]] {
  self : BackupMonitor with BackupThreadManager with BackupErrorHandler =>
  type Source = TreeSource[SourceType]

  def fs: BackupFileSystem
  
  def backup(source: Source, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    processSourceEntry(source, parent, reference)

  private def processSourceEntry(source: Source, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    catchAndHandleException(source) {
      notifyProgressMonitor(source)
      // create tree node
      val target = fs.createAndGetId(parent, source.name)
      // process children
      source.children.foreach { child =>
        val childReference = reference.flatMap(fs.childId(_, child.name))
        executeInThreadPool(processSourceEntry(child, target, childReference))
      }
      // process data
      if (source.hasData) reference.flatMap(fs.fullDataInformation(_)) match {
        case None => ??? // storeLeaf(src, dst)
        case Some(dataInformation) => ??? // evaluateTimeAndSize(src, dst, ref)
      }
    }
    
}


object BackupToNewNodeAlgorithmTryout {
  val backup = new BackupToNewNodeAlgorithm[FileSource] with BackupPluginDummy {
    def fs: BackupFileSystem = dedup.database.StubbedFileSystem
  }
  
  backup.backup(new FileSource(new java.io.File("")), TreeEntryID(0), None)
}