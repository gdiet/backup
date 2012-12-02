package net.diet_rich.backup.algorithm

import net.diet_rich.backup.database._

case class FullDataInformation (
  time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  dataid: Long
)

trait BackupFileSystem extends TreeDB with DataInfoDB with ByteStoreDB
