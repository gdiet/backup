package net.diet_rich.backup.alg

trait DataInfoDB {
  /** @return true if at least one matching data entry is stored. */
  def hasMatch(size: Long, print: Long): Boolean
  /** @return The data id if a matching data entry is stored. */
  def findMatch(size: Long, print: Long, hash: Array[Byte]): Option[Long]
  /** @throws Exception if the entry was not created correctly. */
  def createDataEntry(dataid: Long, size: Long, print: Long, hash: Array[Byte]): Unit
}
