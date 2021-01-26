package dedup2

import dedup2.Level2._

class Level2 extends AutoCloseable {
  def setTime(id: Long, time: Long): Unit = ???
  def size(dataId: Long): Long = ???
  def child(parent: Long, name: String): Option[TreeEntry] = ???
  def children(id: Long): Seq[TreeEntry] = ???
  def delete(treeEntry: TreeEntry): Unit = ???
  def mkDir(parent: Long, name: String): Long = ???
  def mkFile(parent: Long, name: String, time: Long, dataId: Long): Unit = ???
  def update(id: Long, newParent: Long, newName: String): Unit = ???
  override def close(): Unit = ???
}

object Level2 {
  sealed trait TreeEntry2 { def id: Long; def parent: Long; def name: String; def time: Long }
  case class DirEntry2(id: Long, parent: Long, name: String, time: Long) extends TreeEntry2
}
