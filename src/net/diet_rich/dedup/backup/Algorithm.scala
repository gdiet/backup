// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._

trait TreeHandling[SourceType <: TreeSource[SourceType]] {
  self: BackupControl[SourceType] =>

  protected def fs: BackupFileSystem
  
  def backup(source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    processSourceEntry(source, parent, reference)

  private def processSourceEntry(source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
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
        case None => storeData(source, target)
        case Some(referenceData) =>
          if (source.time == referenceData.time && source.size == referenceData.size)
            processMatchingTimeAndSize(source, target, referenceData)
          else
            storeData(source, target)
      }
    }

  // implemented in other pieces of algorithm cake
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation)
  protected def storeData(source: SourceType, target: TreeEntryID)
}
