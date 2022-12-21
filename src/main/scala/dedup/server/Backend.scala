package dedup
package server

import java.util.concurrent.atomic.AtomicLong

final class Backend(settings: Settings) extends util.ClassLogging:
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val handles = Handles(settings.tempPath)

  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: String): Option[TreeEntry] = entry(pathElements(path))
  /** @return The path elements of this file system path. */
  def pathElements(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  /** @return The child entries of the tree entry, an empty [[Seq]] if the parent entry does not exist. */
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)

  /** @return The size of the file, 0 if the file entry does not exist. */
  def size(file: FileEntry): Long = handles.cachedSize(file.id).getOrElse(db.logicalSize(file.dataId))

  /** Create a virtual file handle so read/write operations can be done on the file.
    * For each [[open]] or [[createAndOpen]], a corresponding [[release]] call is required for normal operation. */
  def open(file: FileEntry): Unit = handles.open(file.id, file.dataId)

  /** Releases a virtual file handle. Triggers a write-through if no other handles are open for the file.
    * For each [[open]] or [[createAndOpen]], a corresponding [[release]] call is required for normal operation.
    *
    * @return False if called without corresponding [[open]] or [[createAndOpen]]. */
  def release(fileId: Long): Boolean =
    handles.release(fileId) match
      case Handles.NotOpen => false
      case maybeEntry =>
        // FIXME handle Some() case
        true

  def child(parentId: Long, name: String): Option[TreeEntry] = ???
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = ???
  def setTime(id: Long, newTime: Long): Unit = ???
  def update(id: Long, newParentId: Long, newName: String): Boolean = ???
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = ???
  /** Deletes a tree entry unless it has children.
    * @return [[false]] if the tree entry has children. */
  def deleteChildless(entry: TreeEntry): Boolean = ???
  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] = ???
  def truncate(id: Long, newSize: Long): Boolean = ???
  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. Note that the byte arrays may be kept in memory, so make sure e.g.
    *             using defensive copy (Array.clone) that they are not modified later.
    * @return `false` if called without createAndOpen or open. */
  def write(id: Long, data: Iterator[(Long, Array[Byte])]): Boolean = ???

  /** Provides the requested number of bytes from the referenced file
    * unless end-of-file is reached - in that case stops there.
    *
    * @param fileId        Id of the file to read from.
    * @param offset        Offset in the file to start reading at, must be >= 0.
    * @param requestedSize Number of bytes to read.
    * @param receiver      The receiver of the bytes read.
    * @return The number of bytes read or [[None]] if the file is not open. */
  // Note that previous implementations provided atomic reads, but this is not really necessary...
  def read(fileId: Long, offset: Long, requestedSize: Long)(receiver: Iterator[(Long, Array[Byte])] => Any): Option[Long] =
    handles.get(fileId).map(_.readLock { handle =>
      ???
    })
    ???
//      lazy val fileSize = knownSize.getOrElse(db.logicalSize(dataId))
//      lazy val parts = db.parts(dataId)
//      val data = cachedData.flatMap {
//        case position -> Left(size) => readFromLts(parts, offset, math.min(requestedSize, fileSize - offset))
//        case position -> Right(data) => Iterator(position -> data)
//      }

  /** From the long term store, reads file content defined by `parts`.
    *
    * @param parts    List of (offset, size) defining the file content parts to read.
    *                 `readFrom` + `readSize` should not exceed summed part sizes unless
    *                 `parts` is the empty list that is used for blacklisted entries.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    *         If `parts` is too small, the data is filled up with zeros.
    * @throws IllegalArgumentException if `readFrom` is negative or `readSize` is less than 1.
    */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    ???

  /** Clean up and release resources. */
  def shutdown(): Unit =
    handles.shutdown() // FIXME handle return value
    log.warn("SHUTDOWN NOT COMPLETELY IMPLEMENTED") // FIXME Backend.shutdown code missing
    db.close()
    log.info("Shutdown complete.")
