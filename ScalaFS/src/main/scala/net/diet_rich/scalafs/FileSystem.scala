package net.diet_rich.scalafs
import net.diet_rich.util.fs.{DeleteResult, RenameResult}


object FileSystem {
  val separator = "/"
  val rootPath  = "/"

  def pathElements(path: String): Option[List[String]] = {
    if (!path.startsWith(separator)) None else
    if (path == rootPath) Some(List()) else
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
  def delete(): DeleteResult
  def name: String
  def renameTo(path: String): RenameResult
}

trait Dir extends Node {
  def list: Seq[Node]
  def mkDir(child: String): Boolean
}

trait File extends Node {
  def size: Long
}
