package net.diet_rich.dedup.core.tree.sql

import net.diet_rich.dedup.core.tree.{DataEntry, TreeEntry, TreeBackend}

class SQLTreeBackend(sessionFactory: SQLSession) extends TreeBackend {
  private implicit def session: CurrentSession = sessionFactory.session
  override def entry(id: Long): Option[TreeEntry] = ???
  override def markDeleted(id: Long, deletionTime: Option[Long]): Boolean = ???
  override def dataEntry(dataid: Long): Option[DataEntry] = ???
  override def children(parent: Long): List[TreeEntry] = ???
  override def children(parent: Long, name: String): List[TreeEntry] = ???
  override def childrenWithDeleted(parent: Long): List[TreeEntry] = ???
  override def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = ???
  override def sizeOf(dataid: Long): Option[Long] = ???
  override def change(id: Long, newParent: Long, newName: String, newTime: Option[Long], newData: Option[Long], newDeletionTime: Option[Long]): Option[TreeEntry] = ???
  override def entries(path: String): List[TreeEntry] = ???
  override def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = ???
  override def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = ???
  override def inTransaction[T](f: => T): T = ???
  override def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): TreeEntry = ???
  override def close(): Unit = sessionFactory.close()
}
