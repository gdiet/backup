package net.diet_rich.backup.alg3

import net.diet_rich.util.io._

object BackupAlgorithm3 {

  trait SourceEntry {
    def hasData: Boolean
    def name: String
    def time: Long
    def size: Long
    def children: Iterable[SourceEntry]
    def read[ReturnType]: (SeekReader => ReturnType) => ReturnType
  }
  
  object SourceEntry {
    def apply(file: java.io.File): SourceEntry = new SourceEntry {
      override def hasData: Boolean = file.isFile
      override def name: String = file.getName
      override def time: Long = file.lastModified
      override def size: Long = file.length
      override def children: Iterable[SourceEntry] =
        if (file.isDirectory) file.listFiles.map(x => SourceEntry(x)) else Nil
      override def read[ReturnType]: (SeekReader => ReturnType) => ReturnType =
        using(new java.io.RandomAccessFile(file, "r"))
    }
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
 
}

import BackupAlgorithm3._
class BackupAlgorithm3(executor: Executor, set: BackupSettings, store: BackupTree) {

  // FIXME remove when ready
  def ??? : Nothing = throw new UnsupportedOperationException("Not yet implemented")

  def backupIntoNewDir(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    startBackup(source, store.mkNode(targetParentId, source.name), referenceId)
  
  private def startBackup(src: SourceEntry, dst: Long, ref: Option[Long]): Unit = {
    if (src.hasData) data(src, dst, ref)
    src.children.foreach { sourceChild =>
      val childName = sourceChild.name
      val targetChild = store.mkNode(dst, childName)
      val referenceChild = ref.flatMap(store.child(_, childName))
      executor.execute(startBackup(sourceChild, targetChild, referenceChild))
    }
  }
  
  private def data(src: SourceEntry, dst: Long, ref: Option[Long]): Unit =
    ref match {
    case Some(ref) => dataWithRef(src, dst, ref)
    case None => storeLeaf(src, dst)
    }

  private def dataWithRef(src: SourceEntry, dst: Long, ref: Long): Unit = {
    val refTS = store.timeSize(ref)
    if (src.time != refTS.time || src.size != refTS.size)
      storeLeaf(src, dst)
    else
      dataMatchingTimeAndSize(src, dst, ref)
  }

  private def dataMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: Long): Unit =
    if (set.printForMatch)
      checkPrintMatch(src, dst, ref)
    else
      setDstDataFromRef(dst, ref)

  private def checkPrintMatch(src: SourceEntry, dst: Long, ref: Long): Unit =
    if (store.print(ref) == src.read(set.printCalculator(_)))
      setDstDataFromRef(dst, ref)
    else {
      storeLeaf(src, dst)
    }

  private def setDstDataFromRef(dst: Long, ref: Long): Unit = {
    store.copyTimeSize(ref, dst)
    store.copyPrintHash(ref, dst)
  }
    
  private def storeLeaf(src: SourceEntry, dst: Long): Unit =
    src.read(storeLeaf(src, dst, _))

  private def storeLeaf(src: SourceEntry, dst: Long, reader: SeekReader): Unit = ??? // FIXME
}
