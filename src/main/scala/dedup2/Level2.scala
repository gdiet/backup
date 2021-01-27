package dedup2

class Level2 extends AutoCloseable {
  def setTime(id: Long, time: Long): Unit = () // FIXME
  def size(dataId: Long): Long = 0 // FIXME
  def child(parent: Long, name: String): Option[TreeEntry] = None // FIXME
  def children(id: Long): Seq[TreeEntry] = Seq() // FIXME
  def delete(treeEntry: TreeEntry): Unit = () // FIXME
  def mkDir(parent: Long, name: String): Long = 1 // FIXME
  def mkFile(parent: Long, name: String, time: Long, dataId: Long): Unit = () // FIXME
  def update(id: Long, newParent: Long, newName: String): Unit = () // FIXME
  override def close(): Unit = () // FIXME
}

object Level2 {
}
