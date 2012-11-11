package net.diet_rich.backup.algorithm

/** This implementation requires normal trees without loops. */
trait TraverseNormalTree {
  def tree: BackupTree
  def storeLeaf(src: SourceEntry, dst: Long): Unit
  def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: DataEntry)
  def execute(command: => Unit): Unit
  
  final def backupIntoNewNode(source: SourceEntry, targetParentId: Long, referenceId: Option[Long]): Unit =
    startBackup(source, tree.create(targetParentId, source.name), referenceId)
  
  private def startBackup(src: SourceEntry, dst: Long, ref: Option[Long]): Unit = {
    if (src.hasData) ref.flatMap(tree.data(_)) match {
      case None => storeLeaf(src, dst)
      case Some(ref) => evaluateTimeAndSize(src, dst, ref)
    }
    src.children.foreach { sourceChild =>
      val childName = sourceChild.name
      val targetChild = tree.create(dst, childName)
      val referenceChild = ref.flatMap(tree.childId(_, childName))
      execute(startBackup(sourceChild, targetChild, referenceChild))
    }
  }
}
