package dedup2

import org.slf4j.LoggerFactory

/** Manages currently open files. Forwards everything else to LevelTwo. */
class Level1 extends AutoCloseable {
  private val log = LoggerFactory.getLogger("dedup.Level1") // TODO static loggers are enough

  private def guard[T](msg: String, logger: String => Unit = log.trace)(f: => T): T =
    try f.tap(t => logger(s"$msg -> $t"))
    catch { case e: Throwable => log.error(s"$msg -> ERROR", e); throw e }

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

  override def close(): Unit = { synchronized { files.keys.foreach(release); files = Map() }; two.close() }

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def entry(path: String): Option[TreeEntry] = entry(split(path))

  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(TreeEntry.root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }

  def size(id: Long, dataId: Long): Long = guard(s"size($id, $dataId)", log.info) {
    synchronized(files.get(id)).map(_._2.size).getOrElse(two.size(id, dataId))
  }

  def delete(entry: TreeEntry): Unit = guard(s"delete($entry)", log.info) {
    entry match {
      case file: FileEntry => synchronized(files -= file.dataId)
      case _: DirEntry => // nothing to do
    }
    two.delete(entry.id)
  }

  def createAndOpen(parentId: Long, name: String, time: Long): Long = guard(s"createAndOpen($parentId, $name)", log.info) {
    two.mkFile(parentId, name, time).tap { id =>
      synchronized(files += id -> (1, new DataEntry(-1)))
    }
  }

  def open(file: FileEntry): Unit = guard(s"open($file)", log.info) {
    synchronized {
      import file._
      files += id -> (files.get(id) match {
        case None => 1 -> new DataEntry(dataId, two.size(id, dataId))
        case Some(count -> dataEntry) =>
          if (dataEntry.baseDataId != dataId) log.warn(s"File $id: Mismatch between previous ${dataEntry.baseDataId} and current $dataId.")
          count + 1 -> dataEntry
      })
    }
  }

  def write(id: Long, offset: Long, data: Array[Byte]): Boolean = guard(s"write($id, $offset, size ${data.length})", log.info) {
    synchronized(files.get(id)).map(_._2.write(offset, data)).isDefined
  }

  def truncate(id: Long, size: Long): Boolean =
    guard(s"truncate($id, $size)", log.info) {
      synchronized(files.get(id)).map(_._2.truncate(size)).isDefined
    }

  def read(id: Long, offset: Long, size: Int): Option[Array[Byte]] =
    guard(s"read($id, $offset, $size)", log.info) {
      synchronized(files.get(id)).map(_._2.read(offset, size, two.read(id, _, _, _)))
    }

  def release(id: Long): Boolean = guard(s"release($id)", log.info) {
    val result = synchronized(files.get(id) match {
      case None => None // No handle found, return false.
      case Some(count -> dataEntry) =>
        if (count < 0) log.error(s"Handle count $count for id $id")
        if (count > 1) { files += id -> (count - 1, dataEntry); Some(None) } // Nothing else to do - file is still open.
        else { files -= id; Some(Some(dataEntry)) } // Outside the sync block persist data if necessary.
    })
    result.flatten.foreach(entry => if (entry.written) two.persist(id, entry))
    result.isDefined
  }
}

object Level1 extends App {
  val store = new Level1()
  val root = store.entry("/").get.asInstanceOf[DirEntry]
  val childId = store.createAndOpen(root.id, "test", 0)
  store.write(childId, 0, Array(1,2,3,4,5))
  store.release(childId)
  val file = store.entry("/test").get.asInstanceOf[FileEntry]
  store.open(file)
  println(store.read(file.id, 0, 4096).get.toList)
  store.release(file.id)
}
