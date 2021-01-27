package dedup2

import dedup2.Level1._

/** Manages currently open files. Forwards everything else to LevelTwo.
  * Assumes that files get their own id and dataId immediately when created,
  * and that files get a new dataId immediately when they are updated. */
class Level1 extends AutoCloseable {
  private val two = new Level2()

  /** dataId -> dataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()

  // Proxy methods
  def child(parent: Long, name: String): Option[TreeEntry]     = two.child(parent, name)
  def children(id: Long): Seq[TreeEntry]                       = two.children(id)
  def setTime(id: Long, time: Long): Unit                      = two.setTime(id, time)
  def mkDir(parent: Long, name: String): Long                  = two.mkDir(parent, name)
  def update(id: Long, newParent: Long, newName: String): Unit = two.update(id, newParent, newName)

  // TODO flush data entries
  override def close(): Unit = two.close()

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def entry(path: String): Option[TreeEntry] = entry(split(path))

  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(TreeEntry.root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }

  def size(dataId: Long): Long = synchronized(files.get(dataId)).map(_.size).getOrElse(two.size(dataId))

  def delete(entry: TreeEntry): Unit = {
    entry match {
      case file: FileEntry => synchronized(files -= file.dataId)
      case _: DirEntry => // nothing to do
    }
    two.delete(entry)
  }

  /** The last persisted state of files is copied - without the current modifications. */
  def copyFile(file: FileEntry, newParent: Long, newName: String): Unit =
    two.mkFile(newParent, newName, file.time, file.dataId)
}

object Level1 {
  /** mutable! */
  class DataEntry(baseDataId: Long) {
    def size: Long = 0 // FIXME
  }
}
