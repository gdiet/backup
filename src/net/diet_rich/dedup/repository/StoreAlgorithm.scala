// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import net.diet_rich.dedup.backup._
import net.diet_rich.dedup.database._
import net.diet_rich.util.io._
import net.diet_rich.util.vals.Size

trait StoreAlgorithm {
  type SourceType <: TreeSource[SourceType]
  
  protected val fs: BackupFileSystem
  protected val control: MemoryManager
  protected val settings: BackupSettings
  protected val matchCheck: MatchCheck
  
  def storeAsNewEntry(source: SourceType, parent: TreeEntryID, reference: Option[TreeEntryID]): TreeEntryID = {
    if (!source.hasData)
      fs.createAndGetId(parent, source.name, NodeType.DIR, source.time)
    else
      reference.flatMap(fs.fullDataInformation(_)) match {
        case None => storeData(source, parent)
        case Some(referenceData) =>
          if (source.time == referenceData.time && source.size == referenceData.size)
            processMatchingTimeAndSize(source, parent, referenceData)
          else
            storeData(source, parent)
      }
  }

  protected def processMatchingTimeAndSize(source: SourceType, parent: TreeEntryID, referenceData: FullDataInformation): TreeEntryID =
    matchCheck.processMatchingTimeAndSize(source.reader, referenceData) match {
      case Some((print, reader)) =>
        using(reader)(reader => storeData(source, parent, reader, print))
      case None =>
        fs.createAndGetId(parent, source.name, NodeType.FILE, referenceData.time, referenceData.dataid)
    }
      
  protected def storeData(source: SourceType, parent: TreeEntryID): TreeEntryID = using(source.reader) { reader =>
    storeData(source, parent, reader, fs.dig.calculatePrint(reader))
  }
  
  protected def storeData(source: SourceType, parent: TreeEntryID, reader: SeekReader, print: Print): TreeEntryID =
    if (fs.hasMatch(source.size, print))
      cacheWhileCalcuatingHash(source, parent, reader)
    else
      storeFromReader(source, parent, reader)

  private def cacheWhileCalcuatingHash(source: SourceType, parent: TreeEntryID, reader: SeekReader): TreeEntryID = {
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
        storeFromBytesRead(source, parent, bytes, print, size, hash)
      case (None, _) =>
        // not yet known, not fully cached, re-read fully
        storeFromReader(source, parent, reader)
      case (dataid, _) =>
        // already known
        fs.createAndGetId(parent, source.name, NodeType.FILE, source.time, dataid)
    }
  }

  private def storeFromReader(source: SourceType, parent: TreeEntryID, reader: SeekReader): TreeEntryID = {
    reader.seek(0)
    val (print, (hash, (dataid, size))) = fs.dig.filterPrint(reader) { reader =>
      fs.dig.filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader, settings.storeMethod, source.size)
      }
    }
    fs.createDataEntry(dataid, size, print, hash, settings.storeMethod)
    fs.createAndGetId(parent, source.name, NodeType.FILE, source.time, Some(dataid))
  }

  private def storeFromBytesRead(source: SourceType, parent: TreeEntryID, bytes: Array[Byte], print: Print, size: Size, hash: Hash): TreeEntryID = {
    val dataid = fs.storeAndGetDataId(bytes, size, settings.storeMethod)
    fs.createDataEntry(dataid, size, print, hash, settings.storeMethod)
    fs.createAndGetId(parent, source.name, NodeType.FILE, source.time, Some(dataid))
  }
  
}

trait MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)]
}

class PrintMatchCheck(calculatePrint: SeekReader => Print) extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    calculatePrint(reader) match {
      case referenceData.print => reader.close; None
      case print => Some(print, reader)
    }
}

class NoPrintMatch extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    None
}

class IgnoreMatch(calculatePrint: SeekReader => Print) extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    Some(calculatePrint(reader), reader)
}
