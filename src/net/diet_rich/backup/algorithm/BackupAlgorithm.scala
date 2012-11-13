package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{Reader, SeekReader, fillFrom, readAndDiscardAll}

trait BackupProblem
case class CouldNotCreateEntry(source: SourceEntry, destinationId: Long, cause: CreateFailedCause) extends BackupProblem

trait BackupAlgorithm {
  def ??? = throw new UnsupportedOperationException
  
  def fs: BackupFileSystem
  def executeTreeOperation(command: => Unit): Unit
  def problemsInTreeOperations: Iterable[BackupProblem]
//  def getLargeArray(size: Long): Option[Array[Byte]]
//  def calculatePrintAndReset(reader: SeekReader): Long
//  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
//  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def checkPrintForMatch: Boolean

  final def backupIntoNewNode(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    executeTreeOperation(makeEntry(source, targetParentId, referenceId))
  
  private def makeEntry(src: SourceEntry, dst: Long, ref: Option[Long]): Option[BackupProblem] =
    fs.createAndGetId(dst, src.name) match {
      case Right(child) => processEntry(src, child, ref); None
      case Left(cause) => Some(CouldNotCreateEntry(src, dst, cause))
    }
  
  private def processEntry(src: SourceEntry, dst: Long, ref: Option[Long]): Unit = {
    if (src.hasData) ref.flatMap(fs.fullDataInformation(_)) match {
      case None => storeLeaf(src, dst)
      case Some(ref) => evaluateTimeAndSize(src, dst, ref)
    }
    src.children.foreach { sourceChild =>
      executeTreeOperation(makeEntry(sourceChild, dst, ref))
    }
  }
  
  private def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: FullDataInformation): Unit =
    if (src.time == ref.time && src.size == ref.size)
      processMatchingTimeAndSize(src, dst, ref)
    else
      storeLeaf(src, dst)
      
  private def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: FullDataInformation): Option[UpdateFailedCause] =
    if (checkPrintForMatch)
      checkPrintMatch(src, dst, ref)
    else
      takeAllFromReference(dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: FullDataInformation): Option[UpdateFailedCause] = ??? // {
//    src.read { reader =>
//      val print = calculatePrintAndReset(reader)
//      if (print == ref.print)
//        takeAllFromReference(dst, ref)
//      else
//        storeLeaf(src, dst, reader, print)
//    }
//  }
//
  final def takeAllFromReference(dst: Long, ref: FullDataInformation): Option[UpdateFailedCause] =
    fs.setData(dst, Some(ref))
    
  private def storeLeaf(src: SourceEntry, dst: Long): Unit = src.read {reader =>
    ???
//    storeLeaf(src, dst, reader, calculatePrintAndReset(reader))
  }
//  
//  private def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit =
//    if (fs.hasMatch(src.size, print))
//      cacheWhileCalcuatingHash(src, dst, reader)
//    else
//      storeFromReader(src, dst, reader)
//
//  private def cacheWhileCalcuatingHash(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
//    val cacheSize = src.size + 1
//    val cache = getLargeArray(cacheSize)
//    val (print, (hash, size)) = filterPrint(reader) { reader =>
//      filterHash(reader) { reader =>
//        cache match {
//          case None => readAndDiscardAll(reader)
//          case Some(bytes) => fillFrom(reader, bytes, 0, bytes.length) + readAndDiscardAll(reader)
//        }
//      }
//    }
//    fs.dataid(size, print, hash) match {
//      case Some(dataid) => fs.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
//      case None => cache match {
//        case Some(bytes) if size+1 == cacheSize =>
//          storeFromBytesRead(src, dst, bytes, print, size, hash)
//        case _ =>
//          reader.seek(0)
//          storeFromReader(src, dst, reader)
//      }
//    }
//  }
//
//  private def storeFromReader(src: SourceEntry, dst: Long, reader: SeekReader): Unit = {
//    val (print, (hash, (dataid, size))) = filterPrint(reader) { reader =>
//      filterHash(reader) { reader =>
//        fs.storeAndGetDataIdAndSize(reader)
//      }
//    }
//    fs.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
//  }
//
//  private def storeFromBytesRead(src: SourceEntry, dst: Long, bytes: Array[Byte], print: Long, size: Long, hash: Array[Byte]): Unit = {
//    val dataid = fs.storeAndGetDataId(bytes, size)
//    fs.setData(dst, Some(SimpleDataEntry(src.time, size, print, hash, dataid)))
//  }
  
}
