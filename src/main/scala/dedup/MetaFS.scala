package dedup

import dedup.Database.{DirNode, FileNode}

class MetaFS(connection: java.sql.Connection) {
  private def split(path: String): Array[String] =
    path.split("/").filter(_.nonEmpty)

  private def globPattern(glob: String): String =
    s"\\Q${glob.replaceAll("\\*", "\\\\E.*\\\\Q").replaceAll("\\?", "\\\\E.\\\\Q")}\\E"

  private val db = new Database(connection)

  private var _startOfFreeData = db.startOfFreeData
  def startOfFreeData: Long = _startOfFreeData
  def setStartOfFreeData(newValue: Long): Unit = _startOfFreeData = newValue

  def entry(path: String): Option[Database.TreeNode] =
    split(path).foldLeft[Option[Database.TreeNode]](Some(Database.root)) {
      case (parent, name) => parent.flatMap(node => db.child(node.id, name))
    }

  def globEntry(path: String): Option[(String, Database.TreeNode)] =
    split(path).foldLeft(Option(("": String, Database.root: Database.TreeNode))) {
      case (parent, name) => parent.flatMap { case (resultingPath, node) =>
        val namePattern = globPattern(name)
        db.children(node.id)
          .sortBy(_.name.toLowerCase)(Ordering[String].reverse)
          .find(_.name.matches(namePattern))
          .pipe(_.map(node => s"$resultingPath/${node.name}" -> node))
      }
    }

  def mkDirs(path: String): Option[Long] =
    split(path).foldLeft(Option(0L)) {
      case (None, _) => None
      case (Some(parent), name) =>
        db.child(parent, name) match {
          case Some(dir: DirNode) => Some(dir.id)
          case Some(_: FileNode) => None
          case None => Some(db.addTreeEntry(parent, name, None, None))
        }
    }

  def child(parentId: Long, name: String): Option[Database.TreeNode] =
    db.child(parentId, name)

  def children(parentId: Long): Seq[Database.TreeNode] =
    db.children(parentId)

  def mkEntry(parentId: Long, name: String, lastModified: Option[Long], dataId: Option[Long]): Long =
    db.addTreeEntry(parentId, name, lastModified, dataId)

  def dataEntry(hash: Array[Byte], size: Long): Option[Long] =
    db.dataEntry(hash, size)

  def mkEntry(start: Long, stop: Long, hash: Array[Byte]): Long =
    db.addDataEntry(start, stop, hash)

  def exists(parentId: Long, name: String): Boolean =
    db.child(parentId, name).isDefined

  def delete(id: Long): Boolean =
    db.delete(id)
}
