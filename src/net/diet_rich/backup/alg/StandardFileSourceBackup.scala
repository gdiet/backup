package net.diet_rich.backup.alg

trait StandardFileSourceBackup
	extends StandardFileSourceTreeProcessor
	with StandardFileSourceReferenceEvaluator
	with StandardFileSourcePrintEvaluator
	with StandardFileSourcePrepareStore
	with StandardFileSourceStore
{
  override type BackupFS = TreeDB with DataInfoDB with MixedDB with ByteStoreDB
}
	
trait StandardFileSourceTreeProcessor extends BackupIntoNewBranch[SourceEntry] {
  type BackupFS <: TreeDB
  def fs: BackupFS
  
  /** Handle distribution to multiple threads, progress monitoring,
   *  checking for end conditions, catching exceptions. */
  def executeTreeOperation(source: SourceEntry)(command: => Unit): Unit
  def evaluateReference(src: SourceEntry, dst: Long, ref: Option[Long]): Unit
  
  final override def backupIntoNewBranch(source: SourceEntry, targetParentId: Long, reference: Option[Long]): Unit =
    processEntry(source, targetParentId, reference)
    
  private def processEntry(source: SourceEntry, targetParentId: Long, reference: Option[Long]): Unit =
    executeTreeOperation(source) {
      val target = fs.createAndGetId(targetParentId, source.name)
	  if (source.hasData) evaluateReference(source, target, reference)
	  processEntryChildren(source, target, reference)
    }

  private def processEntryChildren(source: SourceEntry, targetid: Long, reference: Option[Long]): Unit =
    source.children.foreach(child =>
      processEntry(child, targetid, reference.flatMap(fs.child(_, child.name)))
    )
}

trait StandardFileSourceReferenceEvaluator {
  type BackupFS <: MixedDB
  def fs: BackupFS

  def storeLeaf(source: SourceEntry, targetid: Long): Unit
  def processMatchingTimeAndSize(src: SourceEntry, dst: Long, referenceInfo: FullDataInformation): Unit  
  
  final def evaluateReference(source: SourceEntry, targetid: Long, reference: Option[Long]): Unit =
    reference.flatMap(fs.fullDataInformation(_)) match {
      case None => storeLeaf(source, targetid)
      case Some(ref) => evaluateTimeAndSize(source, targetid, ref)
    }
  
  private def evaluateTimeAndSize(source: SourceEntry, targetid: Long, referenceInfo: FullDataInformation): Unit =
    if (source.time == referenceInfo.time && source.size == referenceInfo.size)
      processMatchingTimeAndSize(source, targetid, referenceInfo)
    else
      storeLeaf(source, targetid)
}

trait StandardFileSourcePrintEvaluator {
  import net.diet_rich.util.io.SeekReader
  type BackupFS <: TreeDB
  def fs: BackupFS

  def checkPrintForMatch: Boolean
  def calculatePrintAndReset(reader: SeekReader): Long
  def storeLeaf(source: SourceEntry, targetid: Long, reader: SeekReader, print: Long): Unit
  
  final def processMatchingTimeAndSize(source: SourceEntry, targetid: Long, referenceInfo: FullDataInformation): Unit =
    if (checkPrintForMatch)
      checkPrintMatch(source, targetid, referenceInfo)
    else
      storeDataReference(targetid, referenceInfo.time, referenceInfo.dataid)

  final def storeDataReference(targetid: Long, time: Long, dataid: Long): Unit =
    fs.setData(targetid, time, dataid)
      
  /* Note: This implementation assumes that re-reading from the input is fast. */
  private def checkPrintMatch(source: SourceEntry, targetid: Long, referenceInfo: FullDataInformation): Unit =
    source.read { reader =>
      calculatePrintAndReset(reader) match {
        case referenceInfo.print => storeDataReference(targetid, referenceInfo.time, referenceInfo.dataid)
        case print => storeLeaf(source, targetid, reader, print)
      }
    }
}

trait StandardFileSourcePrepareStore {
  import net.diet_rich.util.io.{fillFrom,readAndDiscardAll}
  import net.diet_rich.util.io.{Reader,SeekReader}
  type BackupFS <: DataInfoDB
  def fs: BackupFS

  def calculatePrintAndReset(reader: SeekReader): Long
  /** @return An array of the requested size or None. */
  def getLargeArray(size: Long): Option[Array[Byte]]
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit
  def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit
  def storeDataReference(targetid: Long, time: Long, dataid: Long): Unit

  final def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
  
  final def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
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
      case Some(dataid) => storeDataReference(dst, src.time, dataid)
      case None => cache match {
        case Some(bytes) if size+1 <= cacheSize =>
          storeFromBytesRead(src, dst, bytes, print, size, hash)
        case _ =>
          reader.seek(0)
          storeFromReader(src, dst, reader)
      }
    }
  }
}

trait StandardFileSourceStore {
  import net.diet_rich.util.io.{Reader,SeekReader}
  type BackupFS <: ByteStoreDB
  def fs: BackupFS

  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def storeDataReference(targetid: Long, time: Long, dataid: Long): Unit
  def createDataEntry(dataid: Long, size: Long, print: Long, hash: Array[Byte]): Unit
  
  final def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
    val (print, (hash, (dataid, size))) = filterPrint(reader) { reader =>
      filterHash(reader) { reader =>
        fs.storeAndGetDataIdAndSize(reader)
      }
    }
    createDataEntry(dataid, size, print, hash)
    storeDataReference(dst, src.time, dataid)
  }

  final def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit = {
    val dataid = fs.storeAndGetDataId(bytes, size)
    createDataEntry(dataid, size, print, hash)
    storeDataReference(dst, src.time, dataid)
  }

}
