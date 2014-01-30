// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._
import net.diet_rich.dedup.datastore.StoreMethods
import net.diet_rich.util.Hash
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class StoreAlgorithm[SourceType <: TreeSource[SourceType]](
  fs: BackupFileSystem,
  memoryManager: MemoryManager,
  storeMethod: Method,
  matchCheck: MatchCheck) {

  def storeAsNewEntry(name: String, source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): TreeEntryID = {
    if (!source.hasData)
      fs.createAndGetId(parent, name, NodeType.DIR, source.time)
    else
      reference.flatMap(fs.fullDataInformation(_)) match {
        case None => storeData(name, source, parent)
        case Some(referenceData) =>
          if (source.time == referenceData.time && source.size == referenceData.size)
            processMatchingTimeAndSize(name, source, parent, referenceData)
          else
            storeData(name, source, parent)
      }
  }
  
  private def processMatchingTimeAndSize(name: String, source: SourceType, parent: TreeEntryID, referenceData: FullDataInformation): TreeEntryID =
    matchCheck.processMatchingTimeAndSize(source.reader, referenceData) match {
      case Some((print, reader)) =>
        using(reader)(reader => storeData(name, source, parent, reader, print))
      case None =>
        fs.createAndGetId(parent, name, NodeType.FILE, referenceData.time, referenceData.dataid)
    }
  
  private def storeData(name: String, source: SourceType, parent: TreeEntryID): TreeEntryID = using(source.reader) { reader =>
    storeData(name, source, parent, reader, fs.dig.calculatePrint(reader))
  }
  
  private def storeData(name: String, source: SourceType, parent: TreeEntryID, reader: SeekReader, print: Print): TreeEntryID =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(name, source, parent, reader)
    else
      storeFromReader(name, source, parent, reader)

  private def cacheWhileCalcuatingHash(name: String, source: SourceType, parent: TreeEntryID, reader: SeekReader): TreeEntryID = {
    reader.seek(0)
    // here, further optimization is possible: If a file is too large to cache,
    // we could at least cache the start of the file. This would of course lead
    // to more complicated hash calculations.
    val cache = memoryManager.getLargeArray(source.size + Size(1))
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
        fs.storeAndGetDataIdAndSize(reader, storeMethod)
      }
    }
    fs.createDataEntry(dataid, size, print, hash, storeMethod)
    fs.createAndGetId(parent, name, NodeType.FILE, source.time, Some(dataid))
  }

  private def storeFromBytesRead(name: String, source: SourceType, parent: TreeEntryID, bytes: Array[Byte], print: Print, size: Size, hash: Hash): TreeEntryID = {
    val dataid = fs.storeAndGetDataId(bytes, size, storeMethod)
    fs.createDataEntry(dataid, size, print, hash, storeMethod)
    fs.createAndGetId(parent, name, NodeType.FILE, source.time, Some(dataid))
  }
}
