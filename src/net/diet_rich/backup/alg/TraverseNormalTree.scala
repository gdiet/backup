package net.diet_rich.backup.alg

import TraverseNormalTree._

object TraverseNormalTree {
  type TargetTree = {
    def createNode(parentId: Long, name: String): Long
  }
  
  type ReferenceTree = {
    def child(parentId: Long, name: String): Option[Long]
  }
  
  type BackupTree = TargetTree with ReferenceTree
}

/** This implementation requires normal trees without loops. */
trait TraverseNormalTree {
  def tree: BackupTree
  def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: Option[Long])
  def execute(command: => Unit): Unit
  
  final def backupIntoNewNode(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    startBackup(source, tree.createNode(targetParentId, source.name), referenceId)
  
  private def startBackup(src: SourceEntry, dst: Long, ref: Option[Long]): Unit = {
    if (src.hasData) evaluateTimeAndSize(src, dst, ref)
    src.children.foreach { sourceChild =>
      val childName = sourceChild.name
      val targetChild = tree.createNode(dst, childName)
      val referenceChild = ref.flatMap(tree.child(_, childName))
      execute(startBackup(sourceChild, targetChild, referenceChild))
    }
  }
}
