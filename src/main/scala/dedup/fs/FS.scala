package dedup.fs

class FS {
  def info(path: Seq[String]): Option[EntryInfo] = path match {
    case Seq() => Some(EntryInfo(None))
    case Seq("hallo") => Some(EntryInfo(None))
    case _ => None
  }
  def list(path: Seq[String]): ListResult = path match {
    case Seq() => DirEntries(LazyList("hallo"))
    case Seq("hallo") => DirEntries(LazyList.empty)
    case _ => NotFound
  }
}

sealed trait ListResult
case object IsFile extends ListResult
case object NotFound extends ListResult
case class DirEntries(entries: LazyList[String]) extends ListResult

case class EntryInfo(fileInfo: Option[FileInfo])
case class FileInfo(size: Long, time: Long)
