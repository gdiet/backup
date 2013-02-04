// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._
import net.diet_rich.dedup.datastore.StoreMethods

trait AlgorithmCommons {
  type SourceType <: TreeSource[SourceType]
  protected def control: BackupControl[SourceType] with MemoryManager
  protected def fs: BackupFileSystem
  protected def settings: BackupSettings
  def shutdown = control.shutdown
}

trait TreeHandling {
  self: PrintMatchCheck with StoreData with AlgorithmCommons =>
  
  def backup(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntry]): Unit =
    processSourceEntry(name, source, parent, reference.map(_.id))

  private def processSourceEntry(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    control.catchAndHandleException(source) {
      control.notifyProgressMonitor(source)
      // store file or directory
      val target = if (source.hasData)
        storeFileEntry(name, source, parent, reference)
      else
        fs.createAndGetId(parent, name, NodeType.DIR, source.time)
      // process children
      source.children.foreach { child =>
        val childReference = reference.flatMap(fs.childId(_, child.name))
        control.executeInThreadPool(processSourceEntry(child.name, child, target, childReference))
      }
    }
  
  private def storeFileEntry(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): TreeEntryID = {
    reference.flatMap(fs.fullDataInformation(_)) match {
      case None => storeData(name, source, parent)
      case Some(referenceData) =>
        if (source.time == referenceData.time && source.size == referenceData.size)
          processMatchingTimeAndSize(name, source, parent, referenceData)
        else
          storeData(name, source, parent)
    }
  }
}

//trait NoPrintMatchCheck[SourceType] {
//  protected def fs: TreeDB
//  
//  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
//    fs.setData(target, referenceData.time, referenceData.dataid)
//}

trait PrintMatchCheck {
  self: StoreData with AlgorithmCommons =>
  
  protected def processMatchingTimeAndSize(name: String, source: SourceType, parent: TreeEntryID, referenceData: FullDataInformation): TreeEntryID =
    using(source.reader) { reader =>
      fs.dig.calculatePrint(reader) match {
        case referenceData.print => fs.createAndGetId(parent, name, NodeType.FILE, referenceData.time, referenceData.dataid)
        case print => storeData(name, source, parent, reader, print)
      }
    }
}

//trait IgnorePrintMatch[SourceType <: TreeSource[SourceType]] {
//  protected def fs: BackupFileSystem
//  
//  protected def processMatchingTimeAndSize(source: SourceType, parent: TreeEntryID, referenceData: FullDataInformation) =
//    using(source.reader) { reader => storeData(source, parent, reader, fs.dig.calculatePrint(reader)) }
//
//  // implemented in other pieces of algorithm cake
//  protected def storeData(source: SourceType, parent: TreeEntryID, reader: SeekReader, print: Print): Unit
//}

trait StoreData {
  self: AlgorithmCommons =>
  
  protected def storeData(name: String, source: SourceType, parent: TreeEntryID): TreeEntryID = using(source.reader) { reader =>
    storeData(name, source, parent, reader, fs.dig.calculatePrint(reader))
  }
  
  protected def storeData(name: String, source: SourceType, parent: TreeEntryID, reader: SeekReader, print: Print): TreeEntryID =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(name, source, parent, reader)
    else
      storeFromReader(name, source, parent, reader)

  private def cacheWhileCalcuatingHash(name: String, source: SourceType, parent: TreeEntryID, reader: SeekReader): TreeEntryID = {
    reader.seek(0)
    // here, further optimization is possible: If a file is too large to cache,
    // we could at least cache the start of the file. This would of course lead
    // to more complicated hash calculations.
    val cache = control.getLargeArray(source.size + Size(1))
    assume(cache.map(_.length == source.size.value + 1).getOrElse(true)) // just to make sure the array is of the requested size
    // read whole file, if possible, into cache, and calculate print and hash
    val (print, (hash, size)) = fs.dig.filterPrint(reader) { reader =>
      fs.dig.filterHash(reader) { reader =>
        Size( cache match {
          case None => readAndDiscardAll(reader)
          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
        } )
      }
    }
    // evaluate match against known entries
    (fs.findMatch(size, print, hash), cache) match {
      case (None, Some(bytes)) if size < Size(bytes.length) =>
        // not yet known, fully cached
        storeFromBytesRead(name, source, parent, bytes, print, size, hash)
      case (None, _) =>
        // not yet known, not fully cached, re-read fully
        storeFromReader(name, source, parent, reader)
      case (dataid, _) =>
        // already known
        fs.createAndGetId(parent, name, NodeType.FILE, source.time, dataid)
    }
  }

  private def storeFromReader(name: String, source: SourceType, parent: TreeEntryID, reader: SeekReader): TreeEntryID = {
    reader.seek(0)
    val (print, (hash, (dataid, size))) = fs.dig.filterPrint(reader) { reader =>
      fs.dig.filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader, settings.storeMethod, source.size)
      }
    }
    fs.createDataEntry(dataid, size, print, hash, settings.storeMethod)
    fs.createAndGetId(parent, name, NodeType.FILE, source.time, Some(dataid))
  }

  private def storeFromBytesRead(name: String, source: SourceType, parent: TreeEntryID, bytes: Array[Byte], print: Print, size: Size, hash: Hash): TreeEntryID = {
    val dataid = fs.storeAndGetDataId(bytes, size, settings.storeMethod)
    fs.createDataEntry(dataid, size, print, hash, settings.storeMethod)
    fs.createAndGetId(parent, name, NodeType.FILE, source.time, Some(dataid))
  }
}
