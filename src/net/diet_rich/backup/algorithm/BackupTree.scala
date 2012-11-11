package net.diet_rich.backup.algorithm

trait DataEntry {
  def time: Long
  def size: Long
  def print: Long
  def hash: Array[Byte]
  def dataid: Long
}

case class SimpleDataEntry(
  time: Long,
  size: Long,
  print: Long,
  hash: Array[Byte],
  dataid: Long
) extends DataEntry

trait BackupTree {
  /** @return The entry ID. */
  def create(parentId: Long, name: String): Long
  /** @return The child's entry ID if any. */
  def childId(parentId: Long, name: String): Option[Long]
  /** @return The node's data information if any. */
  def data(id: Long): Option[DataEntry]
  /** Does nothing if no such node. */
  def setData(id: Long, data: Option[DataEntry]): Unit
  def dataid(size: Long, print: Long, hash: Array[Byte]): Option[Long]
  def hasMatch(size: Long, print: Long): Boolean
}
