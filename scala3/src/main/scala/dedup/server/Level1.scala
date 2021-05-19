package dedup
package server

class Level1(settings: Settings) extends AutoCloseable with util.ClassLogging {
  val backend = Level2(settings)
  export backend.{child, children, close, mkDir, setTime, update}

  def split(path: String)       : Array[String]     = path.split("/").filter(_.nonEmpty)
  def entry(path: String)       : Option[TreeEntry] = entry(split(path))
  def entry(path: Array[String]): Option[TreeEntry] = path.foldLeft(Option[TreeEntry](root)) {
                                                        case (Some(dir: DirEntry), name) => child(dir.id, name)
                                                        case _ => None
                                                      }

  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = 
    backend.mkFile(newParentId, newName, file.time, file.dataId)

  def delete(entry: TreeEntry): Unit =
    watch(s"delete($entry)") {
      // TODO delete from Level1
      backend.delete(entry.id)
    }

  def size(id: Long, dataId: DataId): Long = 0 // FIXME
}
