package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.SeekReader

/** This implementation assumes that re-reading data from the source reader is fast. */
trait ProcessMatchingTimeAndSize {
  def tree: BackupTree
  def checkPrintForMatch: Boolean
  def calculatePrintAndReset(reader: SeekReader): Long
  def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader, print: Long): Unit
  def takeAllFromReference(dst: Long, ref: DataEntry): Unit
  
  final def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: DataEntry): Unit =
    if (checkPrintForMatch)
      checkPrintMatch(src, dst, ref)
    else
      takeAllFromReference(dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: DataEntry): Unit = {
    src.read { reader =>
      val print = calculatePrintAndReset(reader)
      if (print == ref.print)
        takeAllFromReference(dst, ref)
      else
        storeLeaf(src, dst, reader, print)
    }
  }
      
}