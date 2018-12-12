package dedup.fs

import dedup.fs.meta.Database
import dedup.util.sql.{ConnectionProvider, H2}

object FS {
  val root = DirInfo(1)
}
class FS {
  implicit val connection: ConnectionProvider = H2.singleMemoryConnection
  Database.create()

  def info(path: Seq[String]): Option[EntryInfo] =
    path.foldLeft[Option[EntryInfo]](Some(FS.root)) {
      case (Some(DirInfo(parentId)), name) =>
        Database.node(parentId, name) match {
          case Some((id, _, None)) => Some(DirInfo(id))
          case Some((id, changed, Some(data))) =>
            // FIXME fetch the file size
            Some(FileInfo(id, 0, changed.getOrElse(0)))
          case _ => None
        }
      case (None, _) | (Some(_), _) => None
    }

  def list(path: Seq[String]): ListResult =
    info(path) match {
      case Some(DirInfo(id)) => DirEntries(Database.children(id))
      case Some(_: FileInfo) => IsFile
      case None => NotFound
    }
}

sealed trait ListResult
case object IsFile extends ListResult
case object NotFound extends ListResult
case class DirEntries(entries: Seq[String]) extends ListResult

sealed trait EntryInfo
case class DirInfo(id: Long) extends EntryInfo
case class FileInfo(id: Long, size: Long, time: Long) extends EntryInfo
