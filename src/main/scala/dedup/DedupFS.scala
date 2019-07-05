package dedup

import java.sql.Connection

import util.H2

// FIXME synchronization?
class DedupFS(connection: Connection) extends FSInterface {
  private val db = new Database(connection)

  override def entryAt(path: String): Option[FSEntry] = {
    if (!path.startsWith("/")) None
    else {
      val maybeEntry = path.split("/").filter(_.nonEmpty).foldLeft(Option(Database.byParentNameRoot)) {
        case (parent, name) => parent.flatMap(p => db.entryByParentAndName(p.id, name))
      }
      maybeEntry.map { e =>
        if (e.data.isEmpty) DedupDir(db, e.id)
        else DedupFile(e.length.getOrElse(0), e.changed.getOrElse(0), e.data.getOrElse(0))
      }
    }
  }
  override def close(): Unit = db.close()
}

case class DedupDir(db: Database, id: Long) extends FSDir {
  override def childNames: Seq[String] = db.children(id)
}

case class DedupFile(size: Long, lastModifiedMillis: Long, dataId: Long) extends FSFile {
  override def bytes(offset: Long, size: Int): Array[Byte] = Array() // FIXME
}
