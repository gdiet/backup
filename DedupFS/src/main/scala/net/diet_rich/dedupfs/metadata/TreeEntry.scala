package net.diet_rich.dedupfs.metadata

import net.diet_rich.common.someNow

case class TreeEntry (
  key: Long,
  parent: Long,
  name: String,
  changed: Option[Long] = someNow,
  data: Option[Long] = None
)

object TreeEntry {
  val root = TreeEntry(0L, -1L, "", None)
  val rootPath = ""
  val pathSeparator = "/"
  def pathElements(path: String): Array[String] = {
    require(path == rootPath || path.startsWith(pathSeparator), s"malformed path: $path")
    path split pathSeparator drop 1
  }
}
