package dedup
package server

import java.lang.System.{currentTimeMillis => now}

trait      TreeEntry(id: Long, parentId: Long, name: String, time: Long)
case class DirEntry (id: Long, parentId: Long, name: String, time: Long)               extends TreeEntry(id, parentId, name, time)
case class FileEntry(id: Long, parentId: Long, name: String, time: Long, dataId: Long) extends TreeEntry(id, parentId, name, time)

val root = DirEntry(0, 0, "", now)
