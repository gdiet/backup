// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup
import dedup.database._
import dedup.util.io.SeekReader
import dedup.util.io.using


trait BackupToNewNodeAlgorithm[SourceType <: TreeSource[SourceType]]
extends TreeHandling[SourceType]
with BackupControl[SourceType]
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
  protected def fs: BackupFileSystem
  
  protected def storeData(source: SourceType, target: TreeEntryID): Unit = using(source.reader) { reader =>
    storeData(source, target, reader, fs.calculatePrintAndReset(reader))
  }
  
  protected def storeData(source: SourceType, target: TreeEntryID, reader: SeekReader, print: Print): Unit =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(source, target, reader)
    else
      storeFromReader(source, target, reader)

  private def cacheWhileCalcuatingHash(src: SourceType, target: TreeEntryID, reader: SeekReader): Unit = ??? // {
//    val cacheSize = src.size + 1
//    val cache = getLargeArray(cacheSize)
//    assert(cache.map(_.length == cacheSize).getOrElse(true))
//    val (print, (hash, size)) = filterPrint(reader) { reader =>
//      filterHash(reader) { reader =>
//        cache match {
//          case None => readAndDiscardAll(reader)
//          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
//        }
//      }
//    }
//    fs.findMatch(size, print, hash) match {
//      case Some(dataid) => fs.setData(dst, src.time, dataid)
//      case None => cache match {
//        case Some(bytes) if size+1 <= cacheSize =>
//          storeFromBytesRead(src, dst, bytes, print, size, hash)
//        case _ =>
//          reader.seek(0)
//          storeFromReader(src, dst, reader)
//      }
//    }
//  }

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
    with PrintMatchCheck[FileSource]
    with StoreData[FileSource]
  {
    def fs: BackupFileSystem = dedup.database.StubbedFileSystem
  }
  
  backup.backup(new FileSource(new java.io.File("")), TreeEntryID(0), None)
}