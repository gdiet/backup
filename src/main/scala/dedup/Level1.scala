package dedup

/** Manages currently open files. Forwards everything else to LevelTwo. */
class Level1(settings: Settings) extends AutoCloseable with ClassLogging {
  private def guard[T](msg: => String, logger: (=> String) => Unit = log.trace)(f: => T): T = {
    logger(s"$msg ...")
    try f.tap(t => logger(s"... $msg -> $t"))
    catch { case e: Throwable => log.error(s"... $msg -> ERROR", e); throw e }
  }

  /** The store backing Level 1. */
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

  override def close(): Unit = {
    // Release no matter how many file handles are currently open.
    synchronized {
      while(files.nonEmpty) {
        log.info(s"Files to close: ${files.size}")
        files.keys.foreach(release)
      }
    }
    two.close()
  }

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
      // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
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
            if (dataEntry.baseDataId != dataId) log.warn(s"File $id: Mismatch between previous ${dataEntry.baseDataId} and current $dataId.")
            count + 1 -> dataEntry
        })
      }
    }

  /** @param data LazyList(position -> bytes). Providing the complete data as LazyList allows running the update
    *             atomically / synchronized.
    * @return `false` if called without createAndOpen or open. */
  def write(id: Long, data: LazyList[(Long, Array[Byte])]): Boolean =
    guard(s"write($id, data)") {
      synchronized(files.get(id)).map(_._2.write(data)).isDefined
    }

  def truncate(id: Long, size: Long): Boolean =
    guard(s"truncate($id, $size)") {
      synchronized(files.get(id)).map(_._2.truncate(size)).isDefined
    }

  /** @param size Supports sizes larger than the internal size limit for byte arrays.
    * @param sink The sink to write data into. Providing this instead of returning the data read reduces memory
    *             consumption, especially in case of large reads.
    * @return Some(actual size read) or None if called without createAndOpen.
    */
  def read[D: DataSink](id: Long, offset: Long, size: Long, sink: D): Option[Long] =
    guard(s"read($id, $offset, $size)") {
      synchronized(files.get(id)).map { case (_, dataEntry) =>
        val (sizeRead, holes) = dataEntry.read(offset, size, sink)
        holes.foreach { case holeOffset -> holeSize => two.read(id, dataEntry.baseDataId, holeOffset, holeSize, sink) }
        sizeRead
      }
    }

  def release(id: Long): Boolean =
    guard(s"release($id)") {
      val result = synchronized(files.get(id) match {
        case None => None // No handle found, return false.
        case Some(count -> dataEntry) =>
          if (count < 0) log.error(s"Handle count $count for id $id")
          if (count > 1) { files += id -> (count - 1, dataEntry); Some(None) } // Nothing else to do - file is still open.
          else { files -= id; Some(Some(dataEntry)) } // Outside the sync block persist data if necessary.
      })
      result.flatten.foreach(data => if (data.written) two.persist(id, data) else data.close())
      result.isDefined
    }
}
