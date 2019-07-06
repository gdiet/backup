package dedup

import dedup.Database.ByParentNameResult

class StoreFS(connection: java.sql.Connection) {
  private val db = new Database(connection)

  def entryAt(path: String): Option[String] = {
    require(path.startsWith("/"), s"Path does not start with '/': $path")
    val maybeEntry = path.split("/").filter(_.nonEmpty).foldLeft(Option(Database.byParentNameRoot)) {
      case (parent, name) => parent.flatMap(p => db.entryByParentAndName(p.id, name))
    }
    maybeEntry.flatMap {
      case ByParentNameResult(_, Some(time), Some(start), Some(stop)) => Some("file")
      case ByParentNameResult(id, None, None, None) => Some("dir")
      case entry => System.err.println(s"Malformed $entry for $path"); None
    }
  }
}
