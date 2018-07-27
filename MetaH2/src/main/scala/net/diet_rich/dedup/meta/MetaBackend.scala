package net.diet_rich.dedup.meta

/** File system tree and metadata methods. */
trait MetaBackend {
  /** @return The tree entry reachable by the path. */
  def entry(path: String): Option[TreeEntry]
  /** @return The path for a child entry. */
  final def path(path: String, child: String) = s"$path/$child"
}
object MetaBackend {
  val Root = DirEntry(0, -1, "")
}

sealed trait TreeEntry
case class DirEntry  (key: Long, parent: Long, name: String) extends TreeEntry
case class FileEntry (key: Long, parent: Long, name: String, changed: Long, data: Long) extends TreeEntry
