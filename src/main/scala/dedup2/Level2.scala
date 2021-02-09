package dedup2

import dedup2.store.LongTermStore

class Level2 extends AutoCloseable {
  private val con = H2.mem()
  Database.initialize(con)
  private val db = new Database(con)
  private val lts = new LongTermStore

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
  /** Creates a new file with dataId -1. */
  def mkFile(parentId: Long, name: String, time: Long): Long = db.mkFile(parentId, name, time)
  def update(id: Long, newParentId: Long, newName: String): Unit = db.update(id, newParentId, newName)
  def nextDataId: Long = db.nextId

  /** In Level2, DataEntry objects are not mutated. */
  def persist(id: Long, dataEntry: DataEntry): Unit = {
    synchronized(files += id -> (dataEntry +: files.getOrElse(id, Vector())))
    // FIXME start persisting. Don't forget to close DataEntries afterwards
    dataEntry.read(0, ???, readFromLts)
  }

  def size(id: Long, dataId: Long): Long =
    synchronized(files.get(id)).map(_.head.size).getOrElse(db.dataSize(dataId))

  private def readFromLts(dataId: Long, readFrom: Long, readSize: Int): Data = {
    require(readSize > 0, s"Read size $readSize !> 0")
    val readEnd = readFrom + readSize
    val actuallyRead -> data = db.parts(dataId).foldLeft(0L -> Vector.empty[Array[Byte]]) { case (readPosition -> result, chunkStart -> chunkEnd) =>
      val chunkLen = chunkEnd - chunkStart
      if (readPosition + chunkLen <= readFrom) readPosition + chunkLen -> result
      else  if (readPosition >= readEnd) readPosition -> result
      else {
        val skipInChunk = math.max(0, readFrom - readPosition)
        val takeOfChunk = math.min(chunkLen - skipInChunk, readFrom + readSize - readPosition)
        readPosition + skipInChunk + takeOfChunk -> (result :+ lts.read(chunkStart + skipInChunk, takeOfChunk))
      }
    }
    require(actuallyRead <= readSize, s"Actually read $actuallyRead !<= read size $readSize")
    if (actuallyRead >= readSize) data else data :+ new Array((readSize - actuallyRead).toInt)
  }

  def read(id: Long, dataId: Long, offset: Long, size: Int): Data =
    synchronized(files.get(id))
      .map { entries =>
        entries.reverse.foldLeft(readFromLts _) { case (readMethod, entry) =>
          (_, off, siz) => entry.read(off, siz, readMethod)
        }(dataId, offset, size)
      }
      .getOrElse(readFromLts(dataId, offset, size))
}
