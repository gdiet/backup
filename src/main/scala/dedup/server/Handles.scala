package dedup
package server

class Handles extends util.ClassLogging:
  /** file id -> (count, dataId, current, storing). Remember to synchronize. */
  private var files = Map[Long, (Int, DataId, Option[DataEntry], Seq[DataEntry])]()
  private var closing = false

  /** Decrement the open count for a file. On reaching zero, move the current data entry to the storing queue.
    *
    * @return The data entry moved to the storing queue if the storing queue was empty before.
    *         In this case, the returned data entry needs to be enqueued yet. */
  def release(fileId: Long): Option[DataEntry] = synchronized {
    files.get(fileId) match
      case None =>
        log.warn(s"release called for file $fileId that is currently not open.")
        None
      case Some((count, dataId, current, storing)) =>
        if count < 1 then
          log.error(s"Handle count $count for file $fileId.")
          files += fileId -> (1, dataId, current, storing)
          release(fileId)
        else if count > 1 then
          files += fileId -> (count - 1, dataId, current, storing)
          None
        else (dataId, current, storing) match
          case (_, None, Seq()) =>
            files -= fileId
            None
          case (dataId, Some(current), Seq()) =>
            files += fileId -> (0, dataId, None, Seq(current))
            Some(current)
          case (dataId, current, storing) =>
            files += fileId -> (0, dataId, None, current ++: storing)
            None
  }

  /** @return The data entries that still need to be enqueued. */
  def shutdown(): Map[Long, Option[DataEntry]] = synchronized {
    closing = true
    log.debug(s"Shutting down - ${files.size} handles present.")
    val openWriteHandles = files.collect {
      case fileId -> (count, dataId, Some(current), storing) =>
        if count < 1 then log.error(s"Handle count $count for write handle of file $fileId at shutdown.")
        files += fileId -> (1, dataId, Some(current), storing)
        fileId -> release(fileId)
    }
    if openWriteHandles.nonEmpty then
      log.warn(s"Still ${openWriteHandles.size} open write handles when unmounting the file system.")
    // On Windows, it's sort of normal to still have read file handles open when shutting down the file system.
    val openReadHandles = files.values.count(_._1 > 0)
    if openReadHandles > 0 then log.debug(s"Still $openReadHandles open read handles when unmounting the file system.")
    openWriteHandles
  }

  /** Increments the count of the handle, creating it if necessary.
    * @throws IllegalStateException after shutdown has been called. */
  def open(fileId: Long, dataId: DataId): Unit = synchronized {
    if closing then throw new IllegalStateException(s"Attempt to open file $fileId while closing the backend.")
    files += fileId -> (files.get(fileId) match
      case None => (1, dataId, None, Seq())
      case Some((count, storedId, current, storing)) =>
        // FIXME Make sure the data ID is the same, e.g. when persisting updates the data ID
        // Or maybe just warn...?
        ensure("handles.open.dataid.conflict", dataId == storedId, s"File $fileId, open #$count - dataId $dataId differs from previous $storedId.")
        (count + 1, storedId, current, storing)
    )
  }

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = synchronized {
    files.get(fileId).flatMap {
      case (_, _, Some(current), _) => Some(current.size)
      case (_, _, None, head +: _)  => Some(head.size)
      case _ => None
    }
  }
