package dedup

import dedup.Database.ByParentNameResult

// FIXME synchronization?
class ServerFS(connection: java.sql.Connection) extends FSInterface {
  private val db = new Database(connection)

  override def entryAt(path: String): Option[FSEntry] = {
    if (!path.startsWith("/")) None
    else {
      val maybeEntry = path.split("/").filter(_.nonEmpty).foldLeft(Option(Database.byParentNameRoot)) {
        case (parent, name) => parent.flatMap(p => db.entryByParentAndName(p.id, name))
      }
      maybeEntry.map {
        case ByParentNameResult(_, Some(time), Some(start), Some(stop)) => DedupFile(start, stop, time)
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

case class DedupFile(start: Long, stop: Long, lastModifiedMillis: Long) extends FSFile {
  override def size: Long = stop - start
  override def bytes(offset: Long, size: Int): Array[Byte] = {
    val bytesToWrite = math.min(this.size - offset, size).toInt
    Array.fill(bytesToWrite)(65)
  } // FIXME
}
