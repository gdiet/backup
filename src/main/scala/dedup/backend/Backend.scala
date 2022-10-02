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

  /** Use this method for general synchronization. */
  protected inline def sync[T](f: => T): T = synchronized(f)

  /** @throws an [[IllegalStateException]] indicating that the file system is read-only. */
  protected def readOnly = throw new FileSystemReadOnly

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
  
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = readOnly
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit = readOnly
  /** Deletes a tree entry unless it has children. Should be called only for existing entry IDs.
    * @return [[false]] if the tree entry has children. */
  def deleteChildless(entry: TreeEntry): Boolean = readOnly

  // *** File content operations ***
  /** Creates a virtual file handle so read/write operations can be done on the file. */
  def open(file: FileEntry): Unit = { /* No read backend implementation needed. */ }
  /** Releases a virtual file handle. Triggers a write-through if no other handles are open for the file.
    * @return [[false]] if called without create or open. */
  def release(fileId: Long): Boolean = true // No special read backend implementation needed.
