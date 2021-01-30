package dedup2

class Level2 extends AutoCloseable {
  private val con = H2.mem()
  Database.initialize(con)
  private val db = new Database(con)

  /** id -> dataEntries. dataEntries is non-empty. Remember to synchronize. */
  private var files = Map[Long, Vector[DataEntry]]()

  override def close(): Unit = con.close()

  def setTime(id: Long, time: Long): Unit = db.setTime(id, time)
  def dataId(id: Long): Option[Long] = db.dataId(id)
  def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)
  def delete(id: Long): Unit = db.delete(id)
  def mkDir(parentId: Long, name: String): Long = db.mkDir(parentId, name)
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Unit = db.mkFile(parentId, name, time, dataId)
  def mkFile(parentId: Long, name: String, time: Long): Long = db.mkFile(parentId, name, time)
  def update(id: Long, newParentId: Long, newName: String): Unit = db.update(id, newParentId, newName)
  def nextDataId: Long = db.nextId

  /** In Level2, DataEntry objects are not mutated. */
  def persist(id: Long, dataEntry: DataEntry): Unit = // FIXME start persisting
    synchronized(files += id -> (dataEntry +: files.getOrElse(id, Vector())))

  def size(id: Long, dataId: Long): Long =
    synchronized(files.get(id)).map(_.head.size).getOrElse(db.dataSize(dataId))

  def read(id: Long, dataId: Long, offset: Long, size: Int): Array[Byte] = {
    def readFromLts(dataId: Long, offset: Long, size: Int): Array[Byte] = new Array(size) // FIXME
    synchronized(files.get(id))
      .map { entries =>
        entries.reverse.foldLeft(readFromLts _) { case (readMethod, entry) =>
          (_, off, siz) => entry.read(off, siz, readMethod)
        }(dataId, offset, size)
      }
      .getOrElse(readFromLts(dataId, offset, size))
  }
}

object Level2 {
}
