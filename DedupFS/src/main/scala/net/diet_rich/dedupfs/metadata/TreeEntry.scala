package net.diet_rich.dedupfs.metadata

case class TreeEntry (key: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long])

object TreeEntry {
  val root = TreeEntry(0L, -1L, "", None, None)
  val rootPath = ""
  val pathSeparator = "/"

  implicit class RichPath(val path: String) extends AnyVal {
    def / (child: String): String = s"$path$pathSeparator$child"
    def pathElements: Array[String] = {
      require(path == rootPath || path.startsWith(pathSeparator), s"malformed path: $path")
      path split pathSeparator drop 1
    }
    def parent: String = {
      val adapted = if (path endsWith pathSeparator) path dropRight 1 else path
      adapted take (adapted lastIndexOf pathSeparator)
    }
  }
}
