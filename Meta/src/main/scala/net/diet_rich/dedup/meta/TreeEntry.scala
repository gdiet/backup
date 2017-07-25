package net.diet_rich.dedup.meta

case class TreeEntry (key: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long])

object TreeEntry {
  val root = TreeEntry(0L, -1L, "", None, None)
  val rootPath = ""
  val pathSeparator = "/"
}
