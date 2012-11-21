package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.Reader
import net.diet_rich.backup.database.TreeDB
import net.diet_rich.backup.database.DataInfoDB

case class FullDataInformation (
  time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  dataid: Long
)

trait BackupFileSystem extends TreeDB with DataInfoDB {
//  /** @return The child's entry ID if any. */
//  def childId(parentId: Long, name: String): Option[Long]
//  /** @return The matching data id if any. */
//  def dataid(size: Long, print: Long, hash: Array[Byte]): Option[Long]
//  /** @return <code>true</code> if a matching entry is in storage. */
//  def hasMatch(size: Long, print: Long): Boolean
//  
//  def storeAndGetDataIdAndSize(reader: Reader): (Long, Long)
//  def storeAndGetDataId(bytes: Array[Byte], size: Long): Long
// 
}
