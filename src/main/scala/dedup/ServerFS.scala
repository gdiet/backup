package dedup

import dedup.Database.{DirNode, FileNode}

class ServerFS(connection: java.sql.Connection, ds: DataStore) {
  private val meta = new MetaFS(connection)

  def entryAt(path: String): Option[FSEntry] = sync(meta.entry(path).map {
    case dir: DirNode => new FSDir(meta, dir.id)
    case file: FileNode => new FSFile(ds, file.start, file.stop, file.lastModified)
  })
}

sealed trait FSEntry

class FSDir(meta: MetaFS, id: Long) extends FSEntry {
  def childNames: Seq[String] = sync(meta.children(id)).map(_.name)
}

class FSFile(ds: DataStore, start: Long, stop: Long, val lastModifiedMillis: Long) extends FSEntry {
  def size: Long = stop - start
  def bytes(offset: Long, size: Int): Array[Byte] = {
    val bytesToRead = math.min(this.size - offset, size).toInt
    ds.read(start + offset, bytesToRead)
  }
}
