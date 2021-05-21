package dedup
package server

import java.util.concurrent.atomic.AtomicLong

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

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataEntry)]()

  def delete(entry: TreeEntry): Unit =
    watch(s"delete($entry)") {
      // TODO delete from Level1
      backend.delete(entry.id)
    }

  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] =
    watch(s"createAndOpen($parentId, $name)") {
      // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
      // TODO use https://github.com/scala/scala-library-next/pull/80
      backend.mkFile(parentId, name, time).tap(_.foreach { id =>
        synchronized(files += id -> (1, DataEntry(new AtomicLong(-1), 0, settings.tempPath)))
      })
    }

  def size(id: Long, dataId: DataId): Long = 0 // FIXME
}
