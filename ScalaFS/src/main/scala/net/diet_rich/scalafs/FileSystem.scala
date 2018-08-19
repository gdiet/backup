package net.diet_rich.scalafs

object FileSystem {
  val separator = "/"
  val rootPath  = "/"

  def pathElements(path: String): Option[Seq[String]] = {
    if (!path.startsWith(separator)) None else
    if (path == rootPath) Some(Seq()) else
      Some(path.split(separator).toList.drop(1))
  }

  def splitParentPath(path: String): Option[(String, String)] =
    if (!path.startsWith(separator) || path == rootPath) None else {
      Some(path.lastIndexOf('/') match {
        case 0 => ("/", path.drop(1))
        case i => (path.take(i), path.drop(i+1))
      })
    }
}
trait FileSystem {
  def getNode(path: String): Option[Node]
}

sealed trait Node {
  def name: String
}

trait Dir extends Node {
  def list: Seq[Node]
  def mkDir(child: String): Boolean
}

trait File extends Node {
  def size: Long
}
