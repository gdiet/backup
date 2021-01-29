package dedup2

class Level2 extends AutoCloseable {
  private val con = H2.mem()
  Database.initialize(con)
  private val db = new Database(con)
  override def close(): Unit = con.close()

  def setTime(id: Long, time: Long): Unit = db.setTime(id, time)
  def size(id: Long, dataId: Long): Long = 0 // FIXME
  def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)
  def delete(id: Long): Unit = db.delete(id)
  def mkDir(parentId: Long, name: String): Long = db.mkDir(parentId, name)
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Unit = db.mkFile(parentId, name, time, dataId)
  def mkFile(parentId: Long, name: String, time: Long): Long = db.mkFile(parentId, name, time)
  def update(id: Long, newParentId: Long, newName: String): Unit = db.update(id, newParentId, newName)
  def nextDataId: Long = db.nextId
  def persist(dataEntry: Level1.DataEntry): Unit = () // FIXME
}

object Level2 {
}
