package dedup2

sealed trait TreeEntry { def id: Long; def parentId: Long; def name: String; def time: Long }
object TreeEntry { def root: DirEntry = DirEntry(0, 0, "", now) }

case class DirEntry(id: Long, parentId: Long, name: String, time: Long) extends TreeEntry

case class FileEntry(id: Long, parentId: Long, name: String, time: Long, dataId: Long) extends TreeEntry
