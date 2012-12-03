package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{Reader, SeekReader, fillFrom, readAndDiscardAll}

trait StandardFileSourceBackup extends BackupIntoNewBranch[SourceEntry] {
  
  def ??? = throw new UnsupportedOperationException // FIXME ???
  
  def fs: BackupFileSystem
  /** Handle distribution to multiple threads, progress monitoring,
   *  checking for end conditions, catching exceptions. */
  def executeTreeOperation(source: SourceEntry)(command: => Unit): Unit
  /** @return An array of the requested size or None. */
  def getLargeArray(size: Long): Option[Array[Byte]]
  def calculatePrintAndReset(reader: SeekReader): Long
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def checkPrintForMatch: Boolean

  final def backupIntoNewBranch(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    processEntry(source, targetParentId, referenceId)
    
  private def processEntry(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    executeTreeOperation(src) {
      val target = fs.createAndGetId(dst, src.name)
	  processEntryData(src, target, ref)
	  processEntryChildren(src, target, ref)
    }

  private def processEntryChildren(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    src.children.foreach(processEntry(_, dst, ref))
  
  private def processEntryData(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    if (src.hasData) ref.flatMap(fs.fullDataInformation(_)) match {
      case None => storeLeaf(src, dst)
      case Some(ref) => evaluateTimeAndSize(src, dst, ref)
    }
  
  private def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: FullDataInformation): Unit =
    if (src.time == ref.time && src.size == ref.size)
      processMatchingTimeAndSize(src, dst, ref)
    else
      storeLeaf(src, dst)
      
  private def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: FullDataInformation): Unit =
    if (checkPrintForMatch)
      checkPrintMatch(src, dst, ref)
    else
      fs.setData(dst, ref.time, ref.dataid)

  /* Note: This implementation assumes that re-reading from the input is fast. */
  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: FullDataInformation): Unit =
    src.read { reader =>
      calculatePrintAndReset(reader) match {
        case ref.print => fs.setData(dst, ref.time, ref.dataid)
        case print => storeLeaf(src, dst, reader, print)
      }
    }

  private def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
  
  private def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
    if (fs.hasMatch(src.size, print))
      cacheWhileCalcuatingHash(src, dst, reader)
    else
      storeFromReader(src, dst, reader)

  private def cacheWhileCalcuatingHash(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
    val cacheSize = src.size + 1
    val cache = getLargeArray(cacheSize)
    assert(cache.map(_.length == cacheSize).getOrElse(true))
    val (print, (hash, size)) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        cache match {
          case None => readAndDiscardAll(reader)
          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
        }
      }
    }
    fs.findMatch(size, print, hash) match {
      case Some(dataid) => fs.setData(dst, src.time, dataid)
      case None => cache match {
        case Some(bytes) if size+1 <= cacheSize =>
          storeFromBytesRead(src, dst, bytes, print, size, hash)
        case _ =>
          reader.seek(0)
          storeFromReader(src, dst, reader)
      }
    }
  }

  private def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
    val (print, (hash, (dataid, size))) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader)
      }
    }
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(dst, src.time, dataid)
  }

  private def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit = {
    val dataid = fs.storeAndGetDataId(bytes, size)
    fs.createDataEntry(dataid, size, print, hash)
    fs.setData(dst, src.time, dataid)
  }
  
}
