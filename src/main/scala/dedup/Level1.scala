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

  /** @param copy writes bytes from a memory block into a provided byte array. Use all bytes from 0 to size.
    * - copy#1: Read offset in the memory block.
    * - copy#2: Destination byte array to write to.
    * - copy#3: Write offset in the destination byte array.
    * - copy#4: Number of bytes to copy.                       */
  def write(id: Long, offset: Long, size: Long, copy: (Long, Array[Byte], Int, Int) => Unit): Boolean =
    guard(s"write($id, $offset, size $size, copy)") {
      synchronized(files.get(id)).map(_._2.write(offset, size, copy)).isDefined
    }

  def truncate(id: Long, size: Long): Boolean =
    guard(s"truncate($id, $size)") {
      synchronized(files.get(id)).map(_._2.truncate(size)).isDefined
    }

  def read(id: Long, offset: Long, size: Int, write: (Int, Array[Byte], Int, Int) => Unit): Boolean =
    guard(s"read($id, $offset, $size)") {
      synchronized(files.get(id)).exists(_._2.read(offset, size, write, two.read(id, _, _, _)))
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
