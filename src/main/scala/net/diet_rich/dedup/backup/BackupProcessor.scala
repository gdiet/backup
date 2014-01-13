// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._

class BackupProcessor[SourceType <: TreeSource[SourceType]](
	control: BackupControl[SourceType],
    fs: BackupFileSystem,
    storeAlgorithm: StoreAlgorithm[SourceType]) {

  def backup(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntry]): Unit =
    processSourceEntry(name, source, parent, reference.map(_.id))

  private def processSourceEntry(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    control.catchAndHandleException(source) {
      control.notifyProgressMonitor(source)
      // store file or directory
      val target = storeAlgorithm.storeAsNewEntry(name, source, parent, reference)
      // process children if any
      source.children.foreach { child =>
        val childReference = reference.flatMap(fs.childId(_, child.name))
        control.executeInThreadPool(processSourceEntry(child.name, child, target, childReference))
      }
    }

  
}
