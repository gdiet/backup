package dedup
package server

class Handles extends util.ClassLogging:
  /** file id -> (count, dataId, current, storing). Remember to synchronize. */
  private var files = Map[Long, (Int, DataId, Option[DataEntry], Seq[DataEntry])]()
  private var closing = false

  def shutdown(): Unit = synchronized {
    closing = true
    // TODO close write handles
    // On Windows, it's sort of normal to still have read file handles open when shutting down the file system.
    val openReadHandles = files.values.count(_._1 > 0)
    if openReadHandles > 0 then log.debug(s"Still $openReadHandles open read handles when unmounting the file system.")
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
