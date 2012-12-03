package net.diet_rich.backup.alg

case class FullDataInformation (
  time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  dataid: Long
)

trait BackupFileSystem extends TreeDB