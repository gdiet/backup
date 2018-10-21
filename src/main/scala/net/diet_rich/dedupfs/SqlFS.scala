package net.diet_rich.dedupfs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.util.Nel
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
    final def renameTo(path: String): RenameResult = fs {
      SqlFS.pathElements(path).map(meta.entries) match {
        case Some(Nel(Left(_), Right(parent) :: _)) =>
          if (parent.isDir) {
            ??? // FIXME implement
            RenameOk
          } else RenameParentNotADirectory
        case Some(Nel(Left(_), _)) => RenameParentDoesNotExist
        case Some(Nel(Right(_), _)) => RenameTargetExists
        case None => RenameIllegalPath
      }
    }
  }

  class Dir(val entry: meta.TreeEntry) extends Node {
    def delete(): DeleteResult = fs(meta.delete(entry.id))
    def list: Seq[Node] = fs(meta.children(entry.id).map(nodeFor))
    def mkDir(child: String): Boolean = fs(meta.addNewDir(entry.id, child).isDefined)
    override def toString: String = fs(s"Dir '$name': $entry")
  }

  class File(val entry: meta.TreeEntry) extends Node {
    def delete(): DeleteResult = fs(meta.delete(entry.id))
    def size: Long = fs(0) // FIXME
    override def toString: String = fs(s"File '$name': $entry")
  }

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir(entry) else new File(entry)

  def getNode(path: String): Option[Node] = fs {
    SqlFS.pathElements(path).map(meta.entries).flatMap(_.head.toOption.map(nodeFor))
  }

  def mkdir(path: String): MkdirResult = fs {
    SqlFS.pathElements(path).map(meta.entries) match {
      case Some(Nel(Left(_), Right(parent) :: _)) =>
        if (parent.isDir) {
          meta.addNewDir(parent.id, ???)
          ??? // FIXME implement
          MkdirOk
        } else MkdirParentNotADir
      case Some(Nel(Left(_), _)) => MkdirParentNotFound
      case Some(Nel(Right(_), _)) => MkdirExists
      case None => MkdirIllegalPath
    }
  }
}
