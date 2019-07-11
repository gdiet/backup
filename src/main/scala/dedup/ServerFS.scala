package dedup

import dedup.Database.ByParentNameResult

class ServerFS(connection: java.sql.Connection, ds: DataStore) {
  private val db = new Database(connection)

  def entryAt(path: String): Option[FSEntry] = sync(lookup(path, db).map {
    case ByParentNameResult(_, Some(time), Some(start), Some(stop)) => new FSFile(ds, start, stop, time)
    case ByParentNameResult(id, None, None, None) => new FSDir(db, id)
    case entry => System.err.println(s"Malformed $entry for $path"); new FSDir(db, entry.id)
  })

  def close(): Unit = db.close()
}

sealed trait FSEntry

class FSDir(db: Database, id: Long) extends FSEntry {
  def childNames: Seq[String] = sync(db.children(id))
}

class FSFile(ds: DataStore, start: Long, stop: Long, val lastModifiedMillis: Long) extends FSEntry {
  def size: Long = stop - start
  def bytes(offset: Long, size: Int): Array[Byte] = {
    val bytesToRead = math.min(this.size - offset, size).toInt
    ds.read(start, bytesToRead)
  }
}
