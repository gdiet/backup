package net.diet_rich.backup.alg

import net.diet_rich.util.io._

object BackupAlgorithm2 {

trait SourceEntry {
  def isLeaf: Boolean
  def name: String
  def time: Long
  def size: Long
  def children: Iterable[SourceEntry]
  def reader: SeekReader
}

  type TargetTree = {
    def mkNode(parentId: Long, name: String): Long
    def copyTimeSize(sourceId: Long, targetId: Long): Unit
    def copyPrintHash(sourceId: Long, targetId: Long): Unit
  }

  type TimeSize = {
    def time: Long
    def size: Long
  }
  
  type ReferenceTree = {
    def child(parentId: Long, name: String): Option[Long]
    def timeSize(id: Long): TimeSize
    def print(id: Long): Long
  }
  
  type BackupTree = TargetTree with ReferenceTree
  
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

import BackupAlgorithm2._
class BackupAlgorithm2(executor: Executor, set: BackupSettings, store: BackupTree) {

  // FIXME remove when ready
  def ??? : Nothing = throw new UnsupportedOperationException("Not yet implemented")

  def backup(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    twigOrLeaf(source, store.mkNode(targetParentId, source.name), referenceId)
  
  private def twigOrLeaf(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    if (src.isLeaf)
      leaf(src, dst, ref)
    else
      twig(src, dst, ref)

  private def twig(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    src.children.foreach { sourceChild =>
      val childName = sourceChild.name
      val targetChild = store.mkNode(dst, childName)
      val referenceChild = ref.flatMap(store.child(_, childName))
      executor.execute(twigOrLeaf(sourceChild, targetChild, referenceChild))
    }
  
  private def leaf(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    ref match {
    case Some(ref) => leafWithRef(src, dst, ref)
    case None => storeLeaf(src, dst)
    }

  private def leafWithRef(src: SourceEntry, dst: Long, ref: Long): Unit = {
    val refTS = store.timeSize(ref)
    if (src.time != refTS.time || src.size != refTS.size)
      storeLeaf(src, dst)
    else
      leafMatchingTimeAndSize(src, dst, ref)
  }

  private def leafMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: Long): Unit =
    if (set.printForMatch)
      checkPrintMatch(src, dst, ref)
    else
      setDstDataFromRef(dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: Long): Unit =
    if (store.print(ref) == using(src.reader)(set.printCalculator(_)))
      setDstDataFromRef(dst, ref)
    else {
      storeLeaf(src, dst)
    }

  private def setDstDataFromRef(dst: Long, ref: Long): Unit = {
    store.copyTimeSize(ref, dst)
    store.copyPrintHash(ref, dst)
  }
    
  private def storeLeaf(src: SourceEntry, dst: Long): Unit =
    using(src.reader)(storeLeaf(src, dst, _))

  private def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader): Unit = ???
}
