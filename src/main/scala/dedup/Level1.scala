package dedup

/** Manages currently open files. Forwards everything else to LevelTwo. */
class Level1(settings: Settings) extends AutoCloseable with ClassLogging {
  private def guard[T](msg: => String, logger: (=> String) => Unit = trace_)(f: => T): T = {
    logger(s"$msg ...")
    try f.tap(t => logger(s"... $msg -> $t"))
    catch { case e: Throwable => error_(s"... $msg -> ERROR", e); throw e }
  }

  private val two = new Level2(settings)

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataEntry)]()

  // Proxy methods
  def mkDir   (parentId: Long, name: String): Option[Long]          = two.mkDir(parentId, name)
  def child   (parentId: Long, name: String): Option[TreeEntry]     = two.child(parentId, name)
  def children(parentId: Long              ): Seq[TreeEntry]        = two.children(parentId)
  def setTime (id: Long, time: Long): Unit                          = two.setTime(id, time)
  def update  (id: Long, newParentId: Long, newName: String): Unit  = two.update(id, newParentId, newName)
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = two.mkFile(newParentId, newName, file.time, file.dataId)

  override def close(): Unit = { synchronized { files.keys.foreach(release); require(files.isEmpty) }; two.close() }

  def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)

  def entry(path: String): Option[TreeEntry] = entry(split(path))

  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft[Option[TreeEntry]](Some(TreeEntry.root)) {
      case (Some(dir: DirEntry), name) => child(dir.id, name)
      case _ => None
    }

  def size(id: Long, dataId: Long): Long =
    guard(s"size($id, $dataId)") {
      synchronized(files.get(id)).map(_._2.size).getOrElse(two.size(id, dataId))
    }

  def delete(entry: TreeEntry): Unit =
    guard(s"delete($entry)") {
      entry match {
        case file: FileEntry => synchronized {
          files.get(file.id).foreach { case _ -> data => data.close(); files -= file.id }
        }
        case _: DirEntry => // Nothing to do here
      }
      two.delete(entry.id)
    }

  def createAndOpen(parentId: Long, name: String, time: Long): Option[Long] =
    guard(s"createAndOpen($parentId, $name)") {
      two.mkFile(parentId, name, time).tap(_.foreach { id =>
        synchronized(files += id -> (1, new DataEntry(-1, 0, settings.tempPath)))
      })
    }

  def open(file: FileEntry): Unit =
    guard(s"open($file)") {
      synchronized { import file._
        files += id -> (files.get(id) match {
          case None => 1 -> new DataEntry(dataId, two.size(id, dataId), settings.tempPath)
          case Some(count -> dataEntry) =>
            if (dataEntry.baseDataId != dataId) warn_(s"File $id: Mismatch between previous ${dataEntry.baseDataId} and current $dataId.")
            count + 1 -> dataEntry
        })
      }
    }

  def write(id: Long, offset: Long, data: Array[Byte]): Boolean =
    guard(s"write($id, $offset, size ${data.length}, data ${data.take(10).toSeq}...)") {
      synchronized(files.get(id)).map(_._2.write(offset, data)).isDefined
    }

  def truncate(id: Long, size: Long): Boolean =
    guard(s"truncate($id, $size)") {
      synchronized(files.get(id)).map(_._2.truncate(size)).isDefined
    }

  def read(id: Long, offset: Long, size: Int): Option[Data] =
    guard(s"read($id, $offset, $size)") {
      synchronized(files.get(id)).map(_._2.read(offset, size, two.read(id, _, _, _)))
    }

  def release(id: Long): Boolean =
    guard(s"release($id)") {
      val result = synchronized(files.get(id) match {
        case None => None // No handle found, return false.
        case Some(count -> dataEntry) =>
          if (count < 0) error_(s"Handle count $count for id $id")
          if (count > 1) { files += id -> (count - 1, dataEntry); Some(None) } // Nothing else to do - file is still open.
          else { files -= id; Some(Some(dataEntry)) } // Outside the sync block persist data if necessary.
      })
      result.flatten.foreach(data => if (data.written) two.persist(id, data) else data.close())
      result.isDefined
    }
}

object Level1 extends App with ClassLogging { // FIXME remove when not needed anymore
  import java.io.File
  import java.util.concurrent.atomic.AtomicBoolean
  import scala.io.StdIn
  import scala.util.Using.resource

  StdIn.readLine()
  sys.props.update("LOG_BASE", "./")
  val repo = new File("/home/georg/temp/repo").getAbsoluteFile
  def delete(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(delete)
    file.delete()
  }
  delete(repo)
  repo.mkdir()
  val temp = new File(sys.props("java.io.tmpdir") + s"/dedupfs-temp/$now")
  temp.mkdirs()
  val dbDir = Database.dbDir(repo)
  val settings = Settings(repo, dbDir, temp, readonly = false, new AtomicBoolean(false))
  resource(H2.file(dbDir, readonly = false))(Database.initialize)
  val level1 = new Level1(settings)
  try {
    val id = level1.createAndOpen(1, "name", 0).get
    val data = new Array[Byte](4096)
    var start = System.nanoTime()
    (0 to 50000).foreach { n =>
      level1.write(id, n * 4096, data)
      if (n % 1000 == 0) {
        info_(s"${(System.nanoTime() - start)/1000}")
        start = System.nanoTime()
      }
    }
    level1.release(id)
  } finally level1.close()
}
