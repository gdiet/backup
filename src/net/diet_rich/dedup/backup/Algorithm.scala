// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

trait AlgorithmCommons {
  type SourceType <: TreeSource[SourceType]
  protected def control: BackupControl[SourceType] with MemoryManager
  protected def fs: BackupFileSystem
  def shutdown = control.shutdown
}

trait TreeHandling {
  self: PrintMatchCheck with StoreData with AlgorithmCommons =>
  
  def backup(source: SourceType, parent: TreeEntryID, reference: Option[TreeEntry]): Unit =
    processSourceEntry(source, parent, reference.map(_.id))

  private def processSourceEntry(source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): Unit =
    control.catchAndHandleException(source) {
      control.notifyProgressMonitor(source)
      // create tree node
      val target = fs.createAndGetId(parent, source.name)
      // process children
      source.children.foreach { child =>
        val childReference = reference.flatMap(fs.childId(_, child.name))
        control.executeInThreadPool(processSourceEntry(child, target, childReference))
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

trait NoPrintMatchCheck[SourceType] {
  protected def fs: TreeDB
  
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
    fs.setData(target, referenceData.time, referenceData.dataid)
}

trait PrintMatchCheck {
  type SourceType <: TreeSource[SourceType]
  protected def fs: BackupFileSystem
  
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
    using(source.reader) { reader =>
      fs.dig.calculatePrint(reader) match {
        case referenceData.print => fs.setData(target, referenceData.time, referenceData.dataid)
        case print => storeData(source, target, reader, print)
      }
    }

  // implemented in other pieces of algorithm cake
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit
}

trait IgnorePrintMatch[SourceType <: TreeSource[SourceType]] {
  protected def fs: BackupFileSystem
  
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
    using(source.reader) { reader => storeData(source, target, reader, fs.dig.calculatePrint(reader)) }

  // implemented in other pieces of algorithm cake
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit
}

trait StoreData {
  self: AlgorithmCommons =>
  
  protected def storeData(source: SourceType, target: TreeEntryID): Unit = using(source.reader) { reader =>
    storeData(source, target, reader, fs.dig.calculatePrint(reader))
  }
  
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(source, target, reader)
    else
      storeFromReader(source, target, reader)

  private def cacheWhileCalcuatingHash(source: SourceType, target: TreeEntryID, reader: SeekReader): Unit = {
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
        storeFromBytesRead(source, target, bytes, print, size, hash)
      case (None, _) =>
        // not yet known, not fully cached, re-read fully
        storeFromReader(source, target, reader)
      case (dataid, _) =>
        // already known
        fs.setData(target, source.time, dataid)
    }
  }

  private def storeFromReader(source: SourceType, target: TreeEntryID, reader: SeekReader): Unit = {
    reader.seek(0)
    val (print, (hash, (dataid, size))) = fs.dig.filterPrint(reader) { reader =>
      fs.dig.filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader)
      }
    }
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(target, source.time, Some(dataid))
  }

  private def storeFromBytesRead(source: SourceType, target: TreeEntryID, bytes: Array[Byte], print: Print, size: Size, hash: Hash): Unit = {
    val dataid = fs.storeAndGetDataId(bytes, size)
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(target, source.time, Some(dataid))
  }
}
