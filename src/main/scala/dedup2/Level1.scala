package dedup2

import dedup2.Level1._
import org.slf4j.LoggerFactory

/** Manages currently open files. Forwards everything else to LevelTwo.
  * Assumes that files get their own id and dataId immediately when created,
  * and that files get a new dataId immediately when they are updated. */
class Level1 extends AutoCloseable {
  private val log = LoggerFactory.getLogger("dedup.Level1")
  private val two = new Level2()

  /** id -> dataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()
  /** id -> (handle count, dataId). Remember to synchronize. */
  private var handles = Map[Long, (Int, Option[Long])]()

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

  def size(dataId: Long): Long = synchronized(files.get(dataId)).map(_.size).getOrElse(two.size(dataId))

  def delete(entry: TreeEntry): Unit = {
    entry match {
      case file: FileEntry => synchronized(files -= file.dataId)
      case _: DirEntry => // nothing to do
    }
    two.delete(entry.id)
  }

  def createAndOpen(parentId: Long, name: String, time: Long): Long =
    two.mkFile(parentId, name, time).tap { id =>
      synchronized { files += id -> new DataEntry(-1); incHandles(id, None) }
    }

  def open(file: FileEntry): Unit =
    synchronized(incHandles(file.id, Some(file.dataId)))

  /** Remember to synchronize. */
  private def incHandles(id: Long, dataId: Option[Long]): Unit =
    handles += id -> (handles.get(id) match {
      case None => 1 -> dataId
      case Some(count -> storedId) =>
        if (storedId != dataId) log.warn(s"File $id: Mismatch between previous $storedId and current $dataId.")
        count + 1 -> storedId
    })

  private def dataEntry(id: Long, dataId: Option[Long]) =
    synchronized(files.getOrElse(id, new DataEntry(dataId.getOrElse(-1)).tap(files += id -> _)))

  def write(id: Long, offset: Long, size: Long, dataSource: (Long, Int) => Array[Byte]): Boolean =
    synchronized(handles.get(id)) match {
      case None => false
      case Some((_, dataId)) =>
        val data = dataEntry(id, dataId)
        data.write(offset, size, dataSource)
        true
    }

  def truncate(id: Long, size: Long): Boolean =
    synchronized(handles.get(id)) match {
      case None => false
      case Some((_, dataId)) =>
        val data = dataEntry(id, dataId)
        data.truncate(size)
        true
    }

  /** (position, size) => bytes */
  def data(id: Long): Option[(Long, Int) => LazyList[Array[Byte]]] = ???

  def release(id: Long): Boolean = {
    val dataId = synchronized {
      handles.get(id) match {
        case None => Left(())
        case Some(count -> dataId) =>
          if (count < 0) log.error(s"Handle count $count for id $id")
          if (count > 1) { handles += id -> (count-1, dataId); Right(None) }
          else { handles -= id; Right(Some(dataId)) }
      }
    }
    // Left - no handle found, return false.
    // Right(None) - Nothing to do - file is still open.
    // Right(Some(None)) - Need to write, there is no backing entry yet.
    // Right(Some(Some(dataId)) - Need to write, backing entry available.
    dataId.foreach(_.foreach { dataId =>
      val data = dataEntry(id, dataId)
    })
    dataId.isRight
  }
}

object Level1 {
  /** mutable! baseDataId can be -1. */
  class DataEntry(baseDataId: Long) {
    def truncate(size: Long): Unit = () // FIXME
    def write(offset: Long, size: Long, dataSource: (Long, Int) => Array[Byte]): Unit = () // FIXME
    def size: Long = 0 // FIXME
  }
}
