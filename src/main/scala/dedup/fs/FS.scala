package dedup.fs

import dedup.fs.meta.Database
import dedup.util.sql.{ConnectionProvider, H2}

object FS {
  val root = DirInfo(1)
}
class FS {
  implicit val connection: ConnectionProvider = H2.singleMemoryConnection
  Database.create()

  private val treeMonitor = new Object
  def treeLocked[T](f: => T): T = treeMonitor.synchronized(f)

  def info(parentId: Long, name: String): Option[EntryInfo] = {
    Database.node(parentId, name) match {
      case Some((id, _, None)) => Some(DirInfo(id))
      case Some((id, changed, Some(data))) =>
        // FIXME fetch the file size
        Some(FileInfo(id, 0, changed.getOrElse(0)))
      case _ => None
    }
  }

  def info(path: Seq[String]): Option[EntryInfo] =
    path.foldLeft[Option[EntryInfo]](Some(FS.root)) {
      case (Some(DirInfo(parentId)), name) => info(parentId, name)
      case _ => None
    }

  def list(path: Seq[String]): ListResult =
    info(path) match {
      case Some(DirInfo(id)) => DirEntries(Database.children(id))
      case Some(_: FileInfo) => IsFile
      case None => NotFound
    }

  def mkdir(path: Seq[String]): MkdirResult =
    if (path.isEmpty) EntryExists else {
      val parents :+ name = path
      info(parents) match {
        case None => NoSuchDir
        case Some(_: FileInfo) => NotADir
        case Some(DirInfo(parentId)) => if (Database.addNode(parentId, name, None, None)) Created else EntryExists
      }
    }

  def moveRename(id: Long, newParent: Long, newName: String): Boolean =
    Database.moveRename(id, newParent, newName)
}

sealed trait EntryInfo { def id: Long }
case class DirInfo(id: Long) extends EntryInfo
case class FileInfo(id: Long, size: Long, time: Long) extends EntryInfo

sealed trait ListResult
case object IsFile extends ListResult
case object NotFound extends ListResult
case class DirEntries(entries: Seq[String]) extends ListResult

sealed trait MkdirResult
case object EntryExists extends MkdirResult
case object NoSuchDir extends MkdirResult
case object NotADir extends MkdirResult
case object Created extends MkdirResult
