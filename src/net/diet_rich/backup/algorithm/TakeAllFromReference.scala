package net.diet_rich.backup.algorithm

trait TakeAllFromReference {
  def tree: BackupTree

  final def takeAllFromReference(dst: Long, ref: DataEntry): Unit =
    tree.setData(dst, Some(ref))
}