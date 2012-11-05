package net.diet_rich.backup.alg

import BackupAlgorithm._
import EvaluateTimeSizeAgainstReference._

object EvaluateTimeSizeAgainstReference {
  type ReferenceTree = {
    def timeSize(id: Long): TimeSize
  }
  
  type BackupTree = ReferenceTree
}

trait EvaluateTimeSizeAgainstReference {
  def tree: BackupTree
  def storeLeaf(src: SourceEntry, dst: Long): Unit
  def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: Long): Unit
  
  final def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    ref match {
    case Some(ref) => dataWithRef(src, dst, ref)
    case None => storeLeaf(src, dst)
  	}

  private def dataWithRef(src: SourceEntry, dst: Long, ref: Long): Unit = {
    val refTS = tree.timeSize(ref)
    if (src.time != refTS.time || src.size != refTS.size)
      storeLeaf(src, dst)
    else
      processMatchingTimeAndSize(src, dst, ref)
  }
}

