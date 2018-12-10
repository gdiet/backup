package dedup.fs

class FS {
  def info(path: Seq[String]): Option[EntryInfo] = path match {
    case Seq() => Some(EntryInfo(None))
    case Seq("hallo") => Some(EntryInfo(None))
    case _ => None
  }
}

case class EntryInfo(fileInfo: Option[FileInfo])
case class FileInfo(size: Long, time: Long)
