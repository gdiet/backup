package net.diet_rich.backup.algorithm

trait BackupIntoNewBranch[Source] {

  def backupIntoNewBranch(source: Source, targetParentId: Long, referenceId: Option[Long]): Unit
  
}