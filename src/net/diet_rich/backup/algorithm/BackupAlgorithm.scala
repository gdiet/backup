package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.{Reader, SeekReader}

trait BackupAlgorithmInterface {
  def tree: BackupTree
  def execute(command: => Unit): Unit
  def takeAllFromReference(dst: Long, ref: DataEntry): Unit
  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
  def getLargeArray(size: Long): Option[Array[Byte]]
  def calculatePrintAndReset(reader: SeekReader): Long
  def filterPrint[ReturnType](input: Reader)(reader: Reader => ReturnType): (Long, ReturnType)
  def filterHash[ReturnType](input: Reader)(reader: Reader => ReturnType): (Array[Byte], ReturnType)
  def checkPrintForMatch: Boolean

  def backupIntoNewNode(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit
}

trait BackupAlgorithm extends
  BackupAlgorithmInterface with 
  TraverseNormalTree with 
  EvaluateTimeAndSize with 
  ProcessMatchingTimeAndSize with 
  StoreLeaf with
  StoreLeafData
{ }
