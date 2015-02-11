package net.diet_rich.dedup.core.meta

trait TreeBackend {
  private def now = Some(System.currentTimeMillis())

  def entry(id: Long): Option[TreeEntry]
  def childrenWithDeleted(parent: Long): List[TreeEntry]
  def children(parent: Long): List[TreeEntry]
  def children(parent: Long, name: String): List[TreeEntry]
  def entries(path: String): List[TreeEntry]

  def createUnchecked(parent: Long, name: String, changed: Option[Long] = None, dataid: Option[Long] = None): TreeEntry
  def create(parent: Long, name: String, changed: Option[Long] = now, dataid: Option[Long] = None): TreeEntry
  def createWithPath(path: String, changed: Option[Long] = now, dataid: Option[Long] = None): TreeEntry
  def createOrReplace(parent: Long, name: String, changed: Option[Long] = None, dataid: Option[Long] = None): TreeEntry

  def change(id: Long, newParent: Long, newName: String, newTime: Option[Long], newData: Option[Long], newDeletionTime: Option[Long] = None): Option[TreeEntry]
  def markDeleted(id: Long, deletionTime: Option[Long] = now): Boolean

  def dataEntry(dataid: Long): Option[DataEntry]
  def sizeOf(dataid: Long): Option[Long]

  def inTransaction[T](f: => T): T
  def close(): Unit
}
