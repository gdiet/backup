package dedup
package backend

import dedup.db.{ReadDatabase, WriteDatabase}
import dedup.server.Settings

/** @return a [[ReadBackend]] or a [[WriteBackend]] depending on [[Settings.readonly]].
  *         Don't instantiate more than one backend for a repository. */
def apply(settings: Settings): Backend =
  val connection = dedup.db.H2.connection(settings.dbDir, settings.readonly)
  if settings.readonly
  then new ReadBackend(settings, new ReadDatabase(connection))
  else new WriteBackend(settings, new WriteDatabase(connection))

class FileSystemReadOnly extends IllegalStateException("Write attempt on read-only file system")

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
trait Backend:

  def shutdown(): Unit

  /** Used for general synchronization. */
  // Note that if it becomes apparent that synchronization issues cause performance problems,
  // synchronization could be done more fine-grained, e.g. for each DB prepared statement separately.
  protected inline final def sync[T](f: => T): T = synchronized(f)

  /** @throws an [[IllegalStateException]] indicating that the file system is read-only. */
  protected final def readOnly = throw new FileSystemReadOnly

  // *** Tree and meta data operations ***
  
  /** @return The path elements of this file system path. */
  final def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  final def entry(path: String): Option[TreeEntry] = entry(split(path))
  /** @return The [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry]
  /** @return The size of the file. */
  def size(fileEntry: FileEntry): Long
  /** @return The child entries of the tree entry. */
  def children(parentId: Long): Seq[TreeEntry]
  /** @return The named child entry of the tree entry. */
  def child(parentId: Long, name: String): Option[TreeEntry]

  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = readOnly
  /** @return Some(fileId) or None if a child entry with the same name already exists. */
  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] = readOnly
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = readOnly
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit = readOnly
  /** @return `true` if setting new name and parent for the tree entry succeeded. */
  def renameMove(id: Long, newParentId: Long, newName: String): Boolean = readOnly
  /** Deletes a tree entry unless it has children. Should be called only for existing entry IDs.
    * @return [[false]] if the tree entry has children. */
  def deleteChildless(entry: TreeEntry): Boolean = readOnly

  // *** File content operations ***

  /** Creates a virtual file handle so read/write operations can be done on the file. */
  def open(fileId: Long, dataId: DataId): Unit
  
  /** Releases a virtual file handle. Triggers a write-through if no other handles are open for the file.
    * @return [[false]] if called without create or open. */
  def release(fileId: Long): Boolean

  /** Provides the requested number of bytes from the referenced file
    * unless end-of-file is reached - in that case stops there.
    *
    * @param fileId        Id of the file to read from.
    * @param offset        Offset in the file to start reading at, must be >= 0.
    * @param requestedSize Number of bytes to read.
    * @return              A contiguous Iterator(position, bytes) or [[None]] if the file is not open. */
  // Note that previous implementations provided atomic reads, but this is not really necessary...
  def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]]

  /** @param fileId Id of the file to write to.
    * @param data   Iterator(position -> bytes).
    * @return       [[false]] if the file is not open. */
  def write(fileId: Long, data: Iterator[(Long, Array[Byte])]): Boolean = readOnly

  /** @param fileId  Id of the file to truncate.
    * @param newSize The new size of the file, can be more, less or the same as before.
    * @return        [[false]] if the file is not open. */
  def truncate(fileId: Long, newSize: Long): Boolean = readOnly
