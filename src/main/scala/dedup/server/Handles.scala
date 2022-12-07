package dedup
package server

class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:

  /** file id -> (count, dataId, current, storing), encapsulated so setting values can be controlled.
    * Remember to synchronize. */
  private object handles:
    private var map = Map[Long, (Int, DataId, Option[DataEntry2], Seq[DataEntry2])]()
    def apply(): Map[Long, (Int, DataId, Option[DataEntry2], Seq[DataEntry2])] = map
    def set(fileId: Long, count: Int, dataId: DataId, current: Option[DataEntry2], storing: Seq[DataEntry2]): Unit =
      ensure("handles.set.negative", count >= 0, s"Negative handle count $count for file $fileId.")
      ensure("handles.set.nonempty", count > 0 || current.isEmpty, s"Nonempty current entry for file $fileId handle count $count.")
      map += fileId -> (math.max(0, count), dataId, current, storing)
    def remove(fileId: Long): Unit = map -= fileId

  private var closing = false
  private val dataSeq = java.util.concurrent.atomic.AtomicLong()

  /** Decrement the open count for a file. On reaching zero, move the current data entry to the storing queue.
    *
    * @return The data entry moved to the storing queue if the storing queue was empty before.
    *         In this case, the returned data entry needs to be enqueued yet. */
  def release(fileId: Long): Option[DataEntry2] = synchronized {
    handles().get(fileId).fold {
      log.warn(s"release called for file $fileId that is currently not open.")
      None
    }{
      case (0, dataId, current, storing) =>
        log.error(s"Handle count 0 for file $fileId.")
        handles.set(fileId, 1, dataId, current, storing)
        release(fileId)
      case (1, _, None, Seq()) =>
        handles.remove(fileId)
        None
      case (1, dataId, Some(current), storing) =>
        handles.set(fileId, 0, dataId, None, current +: storing)
        if storing.isEmpty then Some(current) else None
      case (count, dataId, current, storing) =>
        handles.set(fileId, count - 1, dataId, current, storing)
        None
    }
  }

  /** @return The data entries that still need to be enqueued. */
  def shutdown(): Map[Long, Option[DataEntry2]] = synchronized {
    closing = true
    log.debug(s"Shutting down - ${handles().size} handles present.")
    val openWriteHandles = handles().collect {
      case fileId -> (_, dataId, Some(current), storing) =>
        handles.set(fileId, 1, dataId, Some(current), storing)
        fileId -> release(fileId)
    }
    if openWriteHandles.nonEmpty then
      log.warn(s"Still ${openWriteHandles.size} open write handles when unmounting the file system.")
    // On Windows, it's sort of normal to still have read file handles open when shutting down the file system.
    val openReadHandles = handles().values.count(_._1 > 0)
    if openReadHandles > 0 then log.debug(s"Still $openReadHandles open read handles when unmounting the file system.")
    openWriteHandles
  }

  /** Increments the count of the handle, creating it if necessary.
    * @throws IllegalStateException after shutdown has been called. */
  def open(fileId: Long, dataId: DataId): Unit = synchronized {
    if closing then throw new IllegalStateException(s"Attempt to open file $fileId while closing the backend.")
    handles().get(fileId) match
      case None => handles.set(fileId, 1, dataId, None, Seq())
      case Some((count, storedId, current, storing)) =>
        // FIXME Make sure the data ID is the same, e.g. when persisting updates the data ID - or maybe just warn...?
        ensure("handles.open.dataid.conflict", dataId == storedId, s"File $fileId, open #$count - dataId $dataId differs from previous $storedId.")
        handles.set(fileId, count + 1, storedId, current, storing)
  }

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = synchronized {
    handles().get(fileId).flatMap {
      case (_, _, Some(current), _) => Some(current.size)
      case (_, _, None, head +: _)  => Some(head.size)
      case _ => None
    }
  }

  /** @return The writable entry or [[None]] if the file is not open. */
  def dataEntry(fileId: Long, sizeInDb: DataId => Long): Option[DataEntry2] = synchronized {
    handles().get(fileId).flatMap {
      case (0, _, _, _) => None
      case (_, _, Some(current), _) => Some(current)
      case (_, dataId, None, storing) =>
        log.trace(s"Creating write cache for $fileId.")
        val initialSize = storing.headOption.map(_.size).getOrElse(sizeInDb(dataId))
        Some(DataEntry2(tempPath.resolve(dataSeq.incrementAndGet().toString), initialSize))
    }
  }
