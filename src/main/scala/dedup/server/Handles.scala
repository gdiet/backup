package dedup
package server

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
  def cachedSize(fileId: Long): Option[Long] =
    synchronized(handles.get(fileId))
      .flatMap { handle => handle.current.orElse(handle.persisting.headOption).map(_.size) }

  /** Create the virtual file handle if missing and increment the handle count.
    * @return `true` if the first handle for the file was created, `false` for subsequent ones. */
  def open(fileId: Long, dataId: DataId): Boolean = synchronized {
    ensure("handles.open", !closing, "Attempt to open file while closing the backend.")
    handles.get(fileId) match
      case None =>
        handles += fileId -> Handle(1, dataId)
        true
      case Some(handle) =>
        // Keep the dataId of the handle - it might be newer/better than the one provided to the method.
        handles += fileId -> handle.copy(count = handle.count + 1)
        false
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
        DataEntry(dataSeq, initialSize, tempPath).tap(handle.withCurrent(_).tap(handles += fileId -> _))
    }).map(_.write(data)).isDefined

  /** Truncates the cached file to a new size. Zero-pads if the file size increases.
    * @return `false` if called without createAndOpen or open. */
  def truncate(fileId: Long, newSize: Long): Boolean =
    synchronized(handles.get(fileId).map {
      case Handle(_, _, Some(current), _) =>
        current.truncate(newSize)
      case handle @ Handle(_, _, None, _) =>
        handles += fileId -> handle.withCurrent(DataEntry(dataSeq, newSize, tempPath))
    }).isDefined

  def get(fileId: Long): Option[Handle] = synchronized(handles.get(fileId))

  def removePersisted(fileId: Long, newDataId: DataId): Unit = synchronized {
    log.trace(s"removePersisted($fileId, $newDataId) - current handles: ${handles.keySet}")
    handles.get(fileId) match
      case None => problem("handles.removeAndGetNext.missing", s"Missing handle for file $fileId.")
      case Some(Handle(0, _, None, Seq(_))) =>
        log.trace(s"Removing handle for $fileId.")
        handles -= fileId
      case Some(handle @ Handle(count, _, _, others :+ _)) =>
        log.debug(s"Keeping handle for $fileId, count $count, persisting ${others.size}.")
        handles += fileId -> handle.copy(dataId = newDataId, persisting = others)
      case Some(handle) =>
        problem("handles.removeAndGetNext.mismatch", s"Persist queue unexpectedly empty in $handle.")
        handles -= fileId
  }

  /** Releases a virtual file handle:
    *  - If the file handle was not open, returns [[Failed]].
    *  - Decrements the handle count for the file.
    *  - If there are more handles on the file, returns [[None]].
    *  - If there was no current [[DataEntry]] write cache, returns [[None]].
    *  - Enqueues the current [[DataEntry]] to the persist queue and returns its size. */
  def release(fileId: Long): Failed | Option[Long] = synchronized {
    handles.get(fileId) match
      case None =>
        log.warn(s"release($fileId) called for a file handle that is currently not open.")
        Failed
      case Some(handle @ Handle(count, dataId, current, persisting)) =>
        if count > 1 then
          handles += fileId -> handle.copy(count - 1)
          None
        else if count == 1 then
          if current.isEmpty && persisting.isEmpty then handles -= fileId
          else handles += fileId -> Handle(0, dataId, None, current ++: persisting)
          current.map(_.size)
        else
          log.warn(s"release($fileId) called for a file having handle count $count.")
          Failed
  }

  /** @return file ID and entry size of entries that still need to be enqueued. */
  def shutdown(): Map[Long, Long] = synchronized {
    closing = true
    handles.flatMap {
      case (fileId, Handle(count, dataId, Some(current), persisting)) =>
        log.warn(s"Modified file $fileId has still $count open handles on shutdown.")
        handles += fileId -> Handle(0, dataId, None, current +: persisting)
        if persisting.isEmpty then Some(fileId -> current.size) else None
      case (fileId, Handle(count, _, None, _)) =>
        if count > 0 then log.info(s"File $fileId has still $count open read handles on shutdown.")
        None
    }
  }
