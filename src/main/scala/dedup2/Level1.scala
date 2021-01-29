package dedup2

import dedup2.Level1._
import org.slf4j.LoggerFactory

/** Manages currently open files. Forwards everything else to LevelTwo. */
class Level1 extends AutoCloseable {
  private val log = LoggerFactory.getLogger("dedup.Level1")
  private val two = new Level2()

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataEntry)]()

  // Proxy methods
  def child(parentId: Long, name: String): Option[TreeEntry]              = two.child(parentId, name)
  def children(parentId: Long): Seq[TreeEntry]                            = two.children(parentId)
  def setTime(id: Long, time: Long): Unit                                 = two.setTime(id, time)
  def mkDir(parentId: Long, name: String): Long                           = two.mkDir(parentId, name)
  def update(id: Long, newParentId: Long, newName: String): Unit          = two.update(id, newParentId, newName)
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Unit = two.mkFile(newParentId, newName, file.time, file.dataId)

  // FIXME flush data entries
  override def close(): Unit = two.close()

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def entry(path: String): Option[TreeEntry] = entry(split(path))

  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(TreeEntry.root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }

  def size(dataId: Long): Long = synchronized(files.get(dataId)).map(_._2.size).getOrElse(two.size(dataId))

  def delete(entry: TreeEntry): Unit = {
    entry match {
      case file: FileEntry => synchronized(files -= file.dataId)
      case _: DirEntry => // nothing to do
    }
    two.delete(entry.id)
  }

  def createAndOpen(parentId: Long, name: String, time: Long): Long =
    two.mkFile(parentId, name, time).tap { id =>
      synchronized(files += id -> (1, new DataEntry(-1)))
    }

  def open(file: FileEntry): Unit =
    synchronized {
      import file._
      files += id -> (files.get(id) match {
        case None => 1 -> new DataEntry(dataId)
        case Some(count -> dataEntry) =>
          if (dataEntry.baseDataId != dataId) log.warn(s"File $id: Mismatch between previous ${dataEntry.baseDataId} and current $dataId.")
          count + 1 -> dataEntry
      })
    }

  def write(id: Long, offset: Long, data: Array[Byte]): Boolean =
    synchronized(files.get(id)).map(_._2.write(offset, data)).isDefined

  def truncate(id: Long, size: Long): Boolean =
    synchronized(files.get(id)).map(_._2.truncate(size)).isDefined

  def read(id: Long, offset: Long, size: Int): Option[Array[Byte]] = ???

  def release(id: Long): Boolean = {
    val result = synchronized(files.get(id) match {
      case None => None
      case Some(count -> dataEntry) =>
        if (count < 0) log.error(s"Handle count $count for id $id")
        if (count > 1) { files += id -> (count - 1, dataEntry); Some(None) }
        else { files -= id; Some(Some(dataEntry)) }
    })
    // None - no handle found, return false.
    // Some(None) - Nothing to do - file is still open.
    // Some(Some(dataEntry)) - Persist data entry if necessary.
    result.flatten.foreach(two.persist)
    result.isDefined
  }
}

object Level1 {
  /** mutable! baseDataId can be -1. */
  class DataEntry(val baseDataId: Long) {
    def data(position: Long, size: Int): LazyList[Array[Byte]] = LazyList(new Array(size)) // FIXME
    def truncate(size: Long): Unit = () // FIXME
    def write(offset: Long, data: Array[Byte]): Unit = () // FIXME
    var size: Long = 0 // FIXME
  }
}
