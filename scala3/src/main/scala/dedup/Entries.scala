package dedup

sealed trait TreeEntry { def id: Long }
case class DirEntry (id: Long, parentId: Long, name: String, time: Long)               extends TreeEntry
case class FileEntry(id: Long, parentId: Long, name: String, time: Long, dataId: Long) extends TreeEntry

val root = DirEntry(0, 0, "", now)
