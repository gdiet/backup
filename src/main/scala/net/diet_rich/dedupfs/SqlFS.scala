package net.diet_rich.dedupfs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.util.{Nel, Head}
import net.diet_rich.util.fs._
import net.diet_rich.util.sql.ConnectionFactory

object SqlFS {
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
class SqlFS {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  protected val meta: H2MetaBackend = new H2MetaBackend

  private def fs[T](t: T): T = synchronized(t)

  sealed trait Node {
    protected def entry: meta.TreeEntry
    final def name: String = fs(entry.name)
  }

  class Dir(val entry: meta.TreeEntry) extends Node {
    override def toString: String = fs(s"Dir '$name': $entry")
  }

  class File(val entry: meta.TreeEntry) extends Node {
    def size: Long = fs(0) // FIXME
    override def toString: String = fs(s"File '$name': $entry")
  }

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir(entry) else new File(entry)

  def delete(path: String, expectDir: Boolean): DeleteResult = fs {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => DeleteBadPath
      case Some(Nel(Left(_), _)) => DeleteNotFound
      case Some(Nel(Right(entry), _)) =>
        if (expectDir) {
          if (!entry.isDir) DeleteFileType
          else if (entry.isDir && meta.children(entry.id).nonEmpty) DeleteHasChildren
          else { meta.delete(entry.id); DeleteOk }
        } else {
          if (entry.isDir) DeleteFileType
          else { meta.delete(entry.id); DeleteOk }
        }
    }
  }

  def getNode(path: String): Option[Node] = fs {
    SqlFS.pathElements(path).map(meta.entries).flatMap(_.head.toOption.map(nodeFor))
  }

  def mkdir(path: String): MkdirResult = fs {
    SqlFS.pathElements(path).fold[MkdirResult](MkdirBadPath)(meta.mkdir)
  }

  def readdir(path: String): ReaddirResult = fs {
    SqlFS.pathElements(path).map(meta.entries) match {
      case None => ReaddirBadPath
      case Some(Nel(Left(_), _)) => ReaddirNotFound
      case Some(Nel(Right(entry), _)) =>
        if (entry.isDir) ReaddirOk(meta.children(entry.id).map(nodeFor))
        else ReaddirNotADirectory
    }
  }

  def rename(oldpath: String, newpath: String): RenameResult = fs {
    (SqlFS.pathElements(oldpath), SqlFS.pathElements(newpath)) match {
      case (None, _) | (_, None) => RenameBadPath
      case (Some(oldNames), Some(newNames)) =>
        meta.entries(oldNames) match {
          case Head(Left(_)) => RenameNotFound
          case Head(Right(entry)) =>
            meta.entries(newNames) match {
              case Nel(Right(target), Right(newParent) :: _) =>
                if (entry.isDir && target.isDir && meta.children(target.id).isEmpty) {
                  meta.delete(target.id)
                  meta.rename(entry.id, target.name, newParent.id)
                } else RenameTargetExists
              case Nel(Left(newName), Right(newParent) :: _) =>
                if (newParent.isDir) meta.rename(entry.id, newName, newParent.id)
                else RenameParentNotADirectory
              case Head(Left(_)) => RenameParentDoesNotExist
            }
        }
    }
  }

  sealed trait ReaddirResult
  case class ReaddirOk(children: Seq[Node]) extends ReaddirResult
  case object ReaddirNotFound extends ReaddirResult
  case object ReaddirBadPath extends ReaddirResult
  case object ReaddirNotADirectory extends ReaddirResult
}
