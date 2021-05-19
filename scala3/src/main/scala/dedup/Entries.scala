package dedup

sealed trait TreeEntry { def id: Long; def name: String }
case class DirEntry (id: Long, parentId: Long, name: String, time: Time)                 extends TreeEntry
case class FileEntry(id: Long, parentId: Long, name: String, time: Time, dataId: DataId) extends TreeEntry

val root = DirEntry(0, 0, "", now)
