package dedup.fs

import dedup.fs.meta.Database
import dedup.util.sql.{ConnectionProvider, H2}

class FS {
  implicit val connection: ConnectionProvider = H2.singleMemoryConnection
  Database.create()

  def info(path: Seq[String]): Option[EntryInfo] =
    path.foldLeft(Option(EntryInfo(1, None))) { // root has id 1
      case (Some(EntryInfo(parentId, None)), name) =>
        Database.node(parentId, name) match {
          case Some((id, _, None)) => Some(EntryInfo(id, None))
          case Some((id, changed, Some(data))) =>
            // FIXME fetch the file size
            Some(EntryInfo(id, Some(FileInfo(0, changed.getOrElse(0)))))
          case _ => None
        }
      case (None, _) | (Some(_), _) => None
    }

  def list(path: Seq[String]): ListResult =
    info(path) match {
      case Some(EntryInfo(id, None)) => DirEntries(Database.children(id).to(LazyList))
      case Some(EntryInfo(_, Some(_))) => IsFile
      case None => NotFound
    }
}

sealed trait ListResult
case object IsFile extends ListResult
case object NotFound extends ListResult
case class DirEntries(entries: LazyList[String]) extends ListResult

case class EntryInfo(id: Long, fileInfo: Option[FileInfo])
case class FileInfo(size: Long, time: Long)
