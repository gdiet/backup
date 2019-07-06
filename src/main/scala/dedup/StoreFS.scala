package dedup

import dedup.Database.ByParentNameResult

class StoreFS(connection: java.sql.Connection) {
  private val db = new Database(connection)

  def mkDirs(path: String): Option[Long] =
    split(path).foldLeft(Option(0L)) {
      case (None, _) => None
      case (Some(parent), name) =>
        db.entryByParentAndName(parent, name) match {
          case Some(ByParentNameResult(id, None, None, None)) => Some(id)
          case None => Some(db.addTreeEntry(parent, name, None, None))
          case _ => None
        }
    }

  // FIXME remove?
  def entryAt(path: String): Option[String] =
    lookup(path, db).flatMap {
      case ByParentNameResult(_, Some(time), Some(start), Some(stop)) => Some("file")
      case ByParentNameResult(id, None, None, None) => Some("dir")
      case entry => System.err.println(s"Malformed $entry for $path"); None
    }
}
