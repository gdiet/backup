package dedup

sealed trait TreeEntry { def id: Long; def parentId: Long; def name: String }
object TreeEntry:
  def apply(id: Long, parentId: Long, name: String, time: Time, dataId: Option[DataId]): TreeEntry =
    dataId.fold(DirEntry(id, parentId, name, time))(FileEntry(id, parentId, name, time, _))
case class DirEntry (id: Long, parentId: Long, name: String, time: Time)                 extends TreeEntry
case class FileEntry(id: Long, parentId: Long, name: String, time: Time, dataId: DataId) extends TreeEntry

val root = DirEntry(0, 0, "", now)
