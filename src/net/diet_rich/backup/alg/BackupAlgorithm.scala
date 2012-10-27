package net.diet_rich.backup.alg

import net.diet_rich.util.io._

trait SourceEntry {
  def isLeaf: Boolean
  def name: String
  def time: Long
  def size: Long
  def children: Iterable[SourceEntry]
  def reader: SeekReader
}

trait TargetEntry {
  def mkNode(name: String): TargetEntry
}

trait ReferenceEntry {
  def child(name: String): Option[ReferenceEntry]
  def time: Long
  def size: Long
  def print: Long
}

object BackupAlgorithm {

  type BackupSettings = {
    def printForMatch: Boolean
    def hashAlgorithm: String
    def printCalculator: (SeekReader) => Long
  }
  
  type Executor = {
    def execute(command: => Unit): Unit
  }
 
  type SourceHandle = {
    def close(): Unit
    def read(bytes: Array[Byte], off: Int, len: Int): Int
  }
  
}

import BackupAlgorithm._
class BackupAlgorithm(executor: Executor, set: BackupSettings) {

  // FIXME remove when ready
  def ??? : Nothing = throw new UnsupportedOperationException("Not yet implemented")
  
  def backupTree(source: SourceEntry, target: TargetEntry, reference: Option[ReferenceEntry]): Unit =
    if (source.isLeaf)
      leaf(source, target, reference)
    else
      twig(source, target, reference)

  private def twig(src: SourceEntry, dst: TargetEntry, ref: Option[ReferenceEntry]): Unit =
    src.children.foreach { sourceChild =>
      val childName = sourceChild.name
      val targetChild = dst.mkNode(childName)
      val referenceChild = ref.flatMap(_.child(childName))
      executor.execute(backupTree(sourceChild, targetChild, referenceChild))
    }
  
  private def leaf(src: SourceEntry, dst: TargetEntry, ref: Option[ReferenceEntry]): Unit =
    ref match {
    case Some(ref) => leafWithRef(src, dst, ref)
    case None => storeLeaf(src, dst)
    }

  private def leafWithRef(src: SourceEntry, dst: TargetEntry, ref: ReferenceEntry): Unit =
    if (src.time != ref.time || src.size != ref.size)
      storeLeaf(src, dst)
    else
      leafMatchingTimeAndSize(src, dst, ref)

  private def leafMatchingTimeAndSize(src: SourceEntry, dst: TargetEntry, ref: ReferenceEntry): Unit =
    if (set.printForMatch)
      checkPrintMatch(src, dst, ref)
    else
      setDstDataFromRef(src, dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: TargetEntry, ref: ReferenceEntry): Unit =
    if (ref.print == using(src.reader)(set.printCalculator(_)))
      setDstDataFromRef(src, dst, ref)
    else {
      storeLeaf(src, dst)
    }

  private def setDstDataFromRef(src: SourceEntry, dst: TargetEntry, ref: ReferenceEntry): Unit =
    ???
    
  private def storeLeaf(src: SourceEntry, dst: TargetEntry): Unit =
    using(src.reader)(storeLeaf(src, dst, _))

  private def storeLeaf(src: SourceEntry, dst: TargetEntry, reader: SeekReader): Unit = ???
}
