package net.diet_rich.backup.alg

trait BackupIntoNewBranch[Source] {

  def backupIntoNewBranch(source: Source, targetParentId: Long, referenceId: Option[Long]): Unit
  
}