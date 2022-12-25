package dedup
package server

object Handles:
  object NotOpen

final case class Handle(count: Int, dataId: DataId, current: Option[DataEntry2] = None, persisting: Seq[DataEntry2] = Seq()):
  /** Prevent race conditions when reading from a persisting entry while at the same time that entry is fully written,
    * gets closed thus becomes unavailable for reading. This race condition can not affect the [[current]] entry because
    * reading requires to hold a file handle preventing [[current]] to be persisted. */
  def readLock[T](f: Handle => T): T =
    persisting.foreach(_.acquire())
    try f(this) finally persisting.foreach(_.release())

/** Manages handles for open files, keeping track of:
  *  - The number of open handles for each file.
  *  - The current write cache for the file if any.
  *  - The persist queue for the file if any. */
final class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:
  private var closing = false
  private val dataSeq = java.util.concurrent.atomic.AtomicLong()

  /** fileId -> [[Handle]]. Remember to synchronize. */
  private var handles = Map[Long, Handle]()

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = None // FIXME implementation missing

  /** Create the virtual file handle if missing and increment the handle count.
    * @return `true` if the first handle for the file was created, `false` for subsequent ones. */
  def open(fileId: Long, dataId: DataId): Boolean = synchronized {
    ensure("handles.open", !closing, "Attempt to open file while closing the backend.")
    val (handle, newlyCreated) = handles.get(fileId) match
      case None => Handle(1, dataId) -> true
      case Some(handle) =>
        ensure("handles.open.dataid.conflict", dataId == handle.dataId, s"Open #${handle.count} - dataId $dataId differs from previous ${handle.dataId}.")
        handle.copy(count = handle.count + 1) -> false
    handles += fileId -> handle
    newlyCreated
  }

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. Note that the byte arrays may be kept in memory, so make sure e.g.
    *             using defensive copy (Array.clone) that they are not modified later.
    * @return `false` if called without createAndOpen or open. */
  def write(fileId: Long, sizeInDb: DataId => Long, data: Iterator[(Long, Array[Byte])]): Boolean =
    synchronized(handles.get(fileId).map {
      case Handle(_, _, Some(current), _) => current
      case handle @ Handle(_, dataId, None, persisting) =>
        log.trace(s"Creating write cache for $fileId.")
        val initialSize = persisting.headOption.map(_.size).getOrElse(sizeInDb(dataId))
        DataEntry2(dataSeq, initialSize, tempPath)
          .tap(current => handle.copy(current = Some(current)).tap(handles += fileId -> _))
    }).map(_.write(data)).isDefined

  /** Truncates the cached file to a new size. Zero-pads if the file size increases.
    * @return `false` if called without createAndOpen or open. */
  def truncate(fileId: Long, newSize: Long): Boolean =
    synchronized(handles.get(fileId).map {
      case Handle(_, _, Some(current), _) =>
        current.truncate(newSize)
      case handle @ Handle(_, _, None, _) =>
        val current = DataEntry2(dataSeq, newSize, tempPath)
        handles += fileId -> handle.copy(current = Some(current))
    }).isDefined

  def get(fileId: Long): Option[Handle] = synchronized(handles.get(fileId))

  /** Releases a virtual file handle:
    *  - If the file was not open, returns [[Handles.NotOpen]].
    *  - Decrements the handle count for the file.
    *  - If there are more handles on the file, returns [[None]].
    *  - If there was no current [[DataEntry2]] write cache, returns [[None]].
    *  - Enqueues the current [[DataEntry2]] to the persist queue.
    *  - If the persist queue was empty before, returns the entry to be handled, otherwise returns [[None]]. */
  def release(fileId: Long): Handles.NotOpen.type | Option[(DataId, DataEntry2)] = synchronized {
    handles.get(fileId) match
      case None =>
        log.warn(s"release($fileId) called for a file handle that is currently not open.")
        Handles.NotOpen
      case Some(handle @ Handle(count, dataId, current, persisting)) =>
        if count < 1 then log.warn(s"release($fileId) called for a file having handle count $count.")
        if count > 1 then { handles += fileId -> handle.copy(count - 1); None }
        else if current.isEmpty then
          if persisting.isEmpty then handles -= fileId else handles += fileId -> handle.copy(count = 0)
          None
        else
          handles += fileId -> Handle(0, dataId, None, current ++: persisting)
          if persisting.isEmpty then current.map(dataId -> _) else None
  }

  /** @return The data entries that still need to be enqueued. */
  def shutdown(): Map[Long, Option[DataEntry2]] = synchronized {
    closing = true
    Map() // FIXME implementation missing
  }
