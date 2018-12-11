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
          case Some((id, None, None)) => Some(EntryInfo(id, None))
          case _ => None
        }
      case _ => None
    }

  def list(path: Seq[String]): ListResult =
    info(path) match {
      case Some(EntryInfo(id, None)) => DirEntries(Database.children(id).to(LazyList))
      case _ => NotFound
    }
}

sealed trait ListResult
case object IsFile extends ListResult
case object NotFound extends ListResult
case class DirEntries(entries: LazyList[String]) extends ListResult

case class EntryInfo(id: Long, fileInfo: Option[FileInfo])
case class FileInfo(size: Long, time: Long)
