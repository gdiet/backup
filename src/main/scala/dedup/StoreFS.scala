package dedup

import dedup.Database.ByParentNameResult

class StoreFS(connection: java.sql.Connection) {
  private val db = new Database(connection)
  private var startOfFreeData = db.startOfFreeData

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

  def mkEntry(parentId: Long, name: String, lastModified: Option[Long], dataId: Option[Long]): Long =
    db.addTreeEntry(parentId, name, lastModified, dataId)

  def dataEntry(hash: Array[Byte], size: Long): Option[Long] =
    db.dataEntry(hash, size)

  def exists(parentId: Long, name: String): Boolean =
    db.entryByParentAndName(parentId, name).isDefined
}
