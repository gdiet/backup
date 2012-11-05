package net.diet_rich.backup.algorithm

trait EvaluateTimeAndSize {
  def tree: BackupTree
  def storeLeaf(src: SourceEntry, dst: Long): Unit
  def processMatchingTimeAndSize(src: SourceEntry, dst: Long, ref: DataEntry): Unit
  
  final def evaluateTimeAndSize(src: SourceEntry, dst: Long, ref: DataEntry): Unit = {
    if (src.time == ref.time && src.size == ref.size)
      processMatchingTimeAndSize(src, dst, ref)
    else
      storeLeaf(src, dst)
  }
}

