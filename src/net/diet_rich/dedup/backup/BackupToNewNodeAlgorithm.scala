// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup
import dedup.database.BackupFileSystem
import dedup.database.FullDataInformation
import dedup.database.Print
import dedup.database.TreeEntryID
import dedup.util.io.SeekReader
import dedup.util.io.using

trait BackupToNewNodeAlgorithm[SourceType <: TreeSource[SourceType]]
extends TreeHandling[SourceType] with BackupControl[SourceType] with PrintMatchCheck[SourceType]

trait TreeHandling[SourceType <: TreeSource[SourceType]] {
  self : BackupControl[SourceType] =>

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
  protected def storeData(source: SourceType, target: TreeEntryID) = ??? // FIXME
}

trait PrintMatchCheck[SourceType <: TreeSource[SourceType]] {
  protected def fs: BackupFileSystem
  
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
    using(source.reader) { reader =>
      fs.calculatePrintAndReset(reader) match {
        case referenceData.print => fs.setData(target, referenceData.time, referenceData.dataid)
        case print => storeData(source, target, reader, print)
      }
    }

  // implemented in other pieces of algorithm cake
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit = ??? // FIXME
}




object BackupToNewNodeAlgorithmTryout {
  val backup = new BackupToNewNodeAlgorithm[FileSource] with BackupControlDummy[FileSource] {
    def fs: BackupFileSystem = dedup.database.StubbedFileSystem
  }
  
  backup.backup(new FileSource(new java.io.File("")), TreeEntryID(0), None)
}