// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup
import dedup.database._
import dedup.util._
import dedup.util.io._


trait BackupToNewNodeAlgorithm[SourceType <: TreeSource[SourceType]]
extends TreeHandling[SourceType]
with BackupControl[SourceType]
with MemoryManager
with PrintMatchCheck[SourceType]
with StoreData[SourceType]


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
  protected def storeData(source: SourceType, target: TreeEntryID)
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
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit
}


trait NoPrintMatchCheck {
  type SourceType
  protected def fs: BackupFileSystem
  
  protected def processMatchingTimeAndSize(source: SourceType, target: TreeEntryID, referenceData: FullDataInformation) =
    fs.setData(target, referenceData.time, referenceData.dataid)
}


trait StoreData[SourceType <: TreeSource[SourceType]] {
  self : MemoryManager =>
  protected def fs: BackupFileSystem
  
  protected def storeData(source: SourceType, target: TreeEntryID): Unit = using(source.reader) { reader =>
    storeData(source, target, reader, fs.calculatePrintAndReset(reader))
  }
  
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(source, target, reader)
    else
      storeFromReader(source, target, reader)

  private def cacheWhileCalcuatingHash(source: SourceType, target: TreeEntryID, reader: SeekReader): Unit = {
    // here, further optimization is possible: If a file is too large to cache,
    // we could at least cache the start of the file.
    val cache = getLargeArray(source.size + 1)
    assume(cache.map(_.length == source.size.value + 1).getOrElse(true)) // just to make sure the array is of the requested size
    // read whole file, if possible, into cache, and calculate print and hash
    val (print, (hash, size)) = fs.filterPrint(reader) { reader =>
      fs.filterHash(reader) { reader =>
        Size( cache match {
          case None => readAndDiscardAll(reader)
          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
        } )
      }
    }
    // evaluate match against known entries
    fs.findMatch(size, print, hash) match {
      // already known
      case Some(dataid) => fs.setData(target, source.time, dataid)
      case None => cache match {
        case Some(bytes) if size <= source.size =>
          // not yet known, fully cached
          storeFromBytesRead(source, target, bytes, print, size, hash)
        case _ =>
          // not yet known, not fully cached, re-read fully
          reader.seek(0)
          storeFromReader(source, target, reader)
      }
    }
  }

  private def storeFromReader(source: SourceType, target: TreeEntryID, reader: SeekReader): Unit = {
    val (print, (hash, (dataid, size))) = fs.filterPrint(reader) { reader =>
      fs.filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader)
      }
    }
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(target, source.time, dataid)
  }

  private def storeFromBytesRead(source: SourceType, target: TreeEntryID, bytes: Array[Byte], print: Print, size: Size, hash: Hash): Unit = {
    val dataid = fs.storeAndGetDataId(bytes, size)
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(target, source.time, dataid)
  }
}


object BackupToNewNodeAlgorithmTryout {
  val backup =
    new BackupToNewNodeAlgorithm[FileSource]
    with BackupControlDummy[FileSource]
    with MemoryManagerDummy
    with PrintMatchCheck[FileSource]
    with StoreData[FileSource]
  {
    def fs: BackupFileSystem = dedup.database.StubbedFileSystem
  }
  
  backup.backup(new FileSource(new java.io.File("")), TreeEntryID(0), None)
}