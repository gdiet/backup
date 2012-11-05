package net.diet_rich.backup.alg

import BackupAlgorithm._
import ProcessMatchingTimeAndSize._

object ProcessMatchingTimeAndSize {
  type ReferenceTree = {
    def print(id: Long): Long
  }
  
  type BackupTree = ReferenceTree
}

trait ProcessMatchingTimeAndSize {
  def tree: BackupTree
  def checkPrintForMatch: Boolean
  def setDataFromReference(dst: Long, ref: Long): Unit
  
  final def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: Long): Unit =
    if (checkPrintForMatch)
      checkPrintMatch(src, dst, ref)
    else
      setDataFromReference(dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: Long): Unit = {
    src.read { reader =>
//      if (tree.print(ref) == set.printCalculator(reader))
//        setDstDataFromRef(dst, ref)
//      else {
//        reader.seek(0)
//        storeLeaf(src, dst, reader) // FIXME re-use already known print
//      }
    }
  }
      
}