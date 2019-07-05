package dedup

import java.sql.Connection

class DedupFS(connection: Connection) extends FSInterface {
  private val db = new Database(connection)
  override def entryAt(path: String): Option[FSEntry] = {
    if (!path.startsWith("/")) None
    else {
      val maybeEntry = path.split("/").filter(_.isEmpty).foldLeft(Option(Database.root)) {
        case (parent, name) => parent.flatMap(p => db.entryByParentName(p.id, name))
      }
      maybeEntry.map {
        case e if e.data.isEmpty => new DedupDir(connection, e.id)
      }
      ???
    }
  }
}

class DedupDir(connection: Connection, id: Long) extends FSDir {
  override def childNames: Seq[String] = ???
}
class DedupFile(connection: Connection, id: Long) extends FSFile {
  override def bytes(offset: Long, size: Int): Array[Byte] = ???
  override def size: Long = ???
}
