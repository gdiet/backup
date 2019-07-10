package dedup

import dedup.Database.ByParentNameResult

// FIXME synchronization?
class ServerFS(connection: java.sql.Connection, ds: Datastore) extends FSInterface {
  private val db = new Database(connection)

  override def entryAt(path: String): Option[FSEntry] = {
    if (!path.startsWith("/")) None
    else {
      lookup(path, db).map {
        case ByParentNameResult(_, Some(time), Some(start), Some(stop)) => DedupFile(ds, start, stop, time)
        case ByParentNameResult(id, None, None, None) => DedupDir(db, id)
        case entry => System.err.println(s"Malformed $entry for $path"); DedupDir(db, entry.id)
      }
    }
  }
  override def close(): Unit = db.close()
}

case class DedupDir(db: Database, id: Long) extends FSDir {
  override def childNames: Seq[String] = db.children(id)
}

case class DedupFile(ds: Datastore, start: Long, stop: Long, lastModifiedMillis: Long) extends FSFile {
  override def size: Long = stop - start
  override def bytes(offset: Long, size: Int): Array[Byte] = {
    val bytesToRead = math.min(this.size - offset, size).toInt
    ds.read(start, bytesToRead)
  }
}
