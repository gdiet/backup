package net.diet_rich.backup.algorithm

trait DataEntry {
  def time: Long
  def size: Long
  def print: Long
  def hash: Array[Byte]
  def dataid: Long
}

case class SimpleDataEntry(time: Long, size: Long, print: Long, hash: Array[Byte], dataid: Long) extends DataEntry

trait BackupTree {
  def createNode(parentId: Long, name: String): Long
  def child(parentId: Long, name: String): Option[Long]
  def data(id: Long): Option[DataEntry]
  def setData(id: Long, data: Option[DataEntry])
  def dataid(size: Long, print: Long, hash: Array[Byte]): Option[Long]
  def hasMatch(size: Long, print: Long): Boolean
}
