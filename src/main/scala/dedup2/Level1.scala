package dedup2

import dedup2.Level1._
import dedup2.Level2.{DirEntry2, TreeEntry2}
import org.slf4j.LoggerFactory

/** Manages currently open files. Forwards everything else to LevelTwo. */
class Level1 extends AutoCloseable {
  private val log = LoggerFactory.getLogger("dedup.Level1")
  private val two = new Level2()

  /** Remember to synchronize. */
  private var files = Map[MapKey, MapValue]()

  override def close(): Unit = {
    // TODO flush files
    two.close()
  }

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def entry(path: String): Option[TreeEntry1] = entry(split(path))

  def entry(path: Array[String]): Option[TreeEntry1] =
    path.foldLeft[Option[TreeEntry1]](Some(root)) {
      case (Some(dir: DirEntry1), name) => child(dir.id, name)
      case _ => None
    }

  def child(parent: Long, name: String): Option[TreeEntry1] =
    synchronized(files.get(MapKey(parent, name)))
      .map(_.asEntry(parent, name))
      .orElse(two.child(parent, name).map(treeEntry1))

  def children(id: Long): Seq[TreeEntry1] = {
    val open = files.collect { case (key, value) if key.parent == id => value.asEntry(key.parent, key.name) }.toSeq
    val openNames = open.map(_.name).toSet
    val other = two.children(id).filterNot(entry => openNames.contains(entry.name)).map(treeEntry1)
    open ++ other
  }

  def size(entry: FileEntry1): Long =
    entry match {
      case open : OpenFileEntry  => open.size
      case other: OtherFileEntry => two.size(other.dataId)
    }

  def setTime(entry: TreeEntry1, time: Long): Unit =
    entry match {
      case entry: HasId1        => two.setTime(entry.id, time)
      case open : OpenFileEntry =>
        val key = MapKey(open.parent, open.name)
        val notSet = synchronized(files.get(key).map(files += key -> _.copy(time = time))).isEmpty
        if (notSet)
          open.id.orElse(two.child(open.parent, open.name).map(_.id)) match {
            case Some(id) => two.setTime(id, time)
            case None     => log.warn(s"Race condition - time not set: $entry.", new Exception())
          }
    }

  def delete(entry: TreeEntry1): Unit = {
    val key = MapKey(entry.parent, entry.name)
    synchronized(files.get(key).tap(_ => files -= key)) -> entry match {
      case Some(value) -> _          => value.id.foreach(two.delete) // If no ID then not yet persisted
      case (_, entry: HasId1)        => two.delete(entry.id)
      case (_, open : OpenFileEntry) =>
        open.id.orElse(two.child(open.parent, open.name).map(_.id)) match {
          case Some(id) => two.delete(id)
          case None     => log.warn(s"Race condition - entry not deleted: $entry", new Exception())
        }
    }
  }

  def copyFile(file: FileEntry1, newParent: Long, newName: String): Unit = {
    val key = MapKey(file.parent, file.name)
    val notCopied = synchronized(files.get(key).map(files += MapKey(newParent, newName) -> _.duplicate)).isEmpty
    if (notCopied) file match {
      case entry: OtherFileEntry => two.copyFile(entry.id, newParent )
    }
    ???
  }

  def mkDir(newParent: Long, newName: String): Long = two.mkDir(newParent, newName)

  def moveRename(entry: TreeEntry1, newParent: Long, newName: String): Unit = ???
}

object Level1 {
  class Data {
    def duplicate: Data = this // TODO
  }

  case class MapKey(parent: Long, name: String)
  case class MapValue(id: Option[Long], time: Long, dataId: Option[Long], size: Long, data: Data) {
    def asEntry(parent: Long, name: String): OpenFileEntry = OpenFileEntry(id, parent, name, time, dataId, size)
    def duplicate: MapValue = copy(id = None, data = data.duplicate)
  }

  val root: DirEntry1 = DirEntry1(0, 0, "", now)

  sealed trait TreeEntry1 { def parent: Long; def name: String; def time: Long }
  sealed trait HasId1 { def id: Long }
  case class DirEntry1(id: Long, parent: Long, name: String, time: Long) extends TreeEntry1 with HasId1
  sealed trait FileEntry1 extends TreeEntry1
  case class OpenFileEntry(id: Option[Long], parent: Long, name: String, time: Long, dataId: Option[Long], size: Long) extends FileEntry1
  case class OtherFileEntry(id: Long, parent: Long, name: String, time: Long, dataId: Long) extends FileEntry1 with HasId1

  def treeEntry1(entry: TreeEntry2): TreeEntry1 = entry match {
    case DirEntry2(id, parent, name, time) => DirEntry1(id, parent, name, time)
  }
}
