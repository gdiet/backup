package dedup
package server

object Handles:
  object NotOpen

final case class Handle(count: Int, dataId: DataId, current: Option[DataEntry2] = None, persisting: Seq[DataEntry2] = Seq()):
  def readLock[T](f: Handle => T): T =
    val handles = (persisting ++ current).tapEach(_.acquire())
    try f(this) finally handles.foreach(_.release())

/** Manages handles for open files, keeping track of:
  *  - The number of open handles for each file.
  *  - The current write cache for the file if any.
  *  - The persist queue for the file if any. */
final class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:
  private var closing = false

  /** fileId -> [[Handle]]. Remember to synchronize. */
  private var files = Map[Long, Handle]()

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = None // FIXME implementation missing

  /** Create the virtual file handle if missing and increment the handle count. */
  def open(fileId: Long, dataId: DataId): Unit = synchronized {
    ensure("handles.open", !closing, "Attempt to open file while closing the backend.")
    files += fileId -> (files.get(fileId) match
      case None => Handle(1, dataId)
      case Some(handle) =>
        ensure("handles.open.dataid.conflict", dataId == handle.dataId, s"Open #${handle.count} - dataId $dataId differs from previous ${handle.dataId}.");
        handle.copy(count = handle.count + 1)
    )
  }

  def get(fileId: Long): Option[Handle] = synchronized(files.get(fileId))

  /** Releases a virtual file handle:
    *  - If the file was not open, returns [[Handles.NotOpen]].
    *  - Decrements the handle count for the file.
    *  - If there are more handles on the file, returns [[None]].
    *  - If there was no [[DataEntry2]] write cache, returns [[None]].
    *  - Enqueues the [[DataEntry2]] to the persist queue.
    *  - If the persist queue was empty before, returns the entry to be handled, otherwise returns [[None]]. */
  def release(fileId: Long): Handles.NotOpen.type | Option[(DataId, DataEntry2)] =
    synchronized(files.get(fileId)) match
      case None => Handles.NotOpen
      case _ => ???
    None // FIXME implementation missing

  /** @return The data entries that still need to be enqueued. */
  def shutdown(): Map[Long, Option[DataEntry2]] = synchronized {
    closing = true
    Map() // FIXME implementation missing
  }
