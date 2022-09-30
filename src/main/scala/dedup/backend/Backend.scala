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
trait Backend extends AutoCloseable:

  /** Use this method for general synchronization. */
  protected inline def sync[T](f: => T): T = synchronized(f)

  /** @throws an [[IllegalStateException]] indicating that the file system is read-only. */
  protected def readOnly = throw new FileSystemReadOnly

  /** [[WriteBackend]]: Write the cache and compact the database. [[ReadBackend]]: Nothing to do. */
  override def close(): Unit = { /**/ }

  // *** Tree and meta data operations ***
  
  /** @return the path elements of this file system path. */
  final def split(path: String): Array[String] = path.split("/").filter(_.nonEmpty)
  /** @return the [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  final def entry(path: String): Option[TreeEntry] = entry(split(path))
  /** @return the [[TreeEntry]] denoted by the file system path or [[None]] if there is no matching entry. */
  def entry(path: Array[String]): Option[TreeEntry]
  /** @return the size of the file. */
  def size(fileEntry: FileEntry): Long

  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = readOnly
  def children(parentId: Long): Seq[TreeEntry]
