package dedup
package server

import java.util.concurrent.atomic.AtomicLong

final class Backend(settings: Settings) extends util.ClassLogging:
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))

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

  /** @return The child entries of the tree entry. */
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)

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
  def open(file: FileEntry): Unit = ???
  def size(file: FileEntry): Long = ???
  def truncate(id: Long, newSize: Long): Boolean = ???
  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. Note that the byte arrays may be kept in memory, so make sure e.g.
    *             using defensive copy (Array.clone) that they are not modified later.
    * @return `false` if called without createAndOpen or open. */
  def write(id: Long, data: Iterator[(Long, Array[Byte])]): Boolean = ???
  /** Reads bytes from the referenced file and writes them to `sink`.
    * Reads the requested number of bytes unless end-of-file is reached
    * first, in that case stops there.
    *
    * Note: Providing a `sink` instead of returning the data enables
    * atomic reads in the - at [[Backend]] mutable - [[DataEntry]] without
    * incurring the risk of large memory allocations.
    *
    * @param id     id of the file to read from.
    * @param offset offset in the file to start reading at.
    * @param size   number of bytes to read, NOT limited by the internal size limit for byte arrays.
    * @param sink   sink to write data to, starting at sink position 0.
    *
    * @return Some(actual size read) or None if called without createAndOpen.
    */
  def read[D: DataSink](id: Long, offset: Long, size: Long, sink: D): Option[Long] = ???
  def release(id: Long): Boolean = ???

  /** Clean up and release resources. */
  def shutdown(): Unit =
    log.warn("SHUTDOWN NOT COMPLETELY IMPLEMENTED") // FIXME Backend.shutdown code missing
    db.close()
    log.info("Shutdown complete.")
