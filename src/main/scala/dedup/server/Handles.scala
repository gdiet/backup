package dedup
package server

object Handles:
  object NotOpen

// FIXME implementation missing
/** Manages handles for open files, keeping track of:
  *  - The number of open handles for each file.
  *  - The current write cache for the file if any.
  *  - The persist queue for the file if any. */
final class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = None

  /** Create the virtual file handle if missing and increment the handle count. */
  def open(fileId: Long, dataId: DataId): Unit = ()

  /** Releases a virtual file handle:
    *  - If the file was not open, returns [[Handles.NotOpen]].
    *  - Decrements the handle count for the file.
    *  - If there are more handles on the file, returns [[None]].
    *  - If there was no [[DataEntry2]] write cache, returns [[None]].
    *  - Enqueues the [[DataEntry2]] to the persist queue.
    *  - If the persist queue was empty before, returns the entry to be handled, otherwise returns [[None]]. */
  def release(fileId: Long): Handles.NotOpen.type | Option[(DataId, DataEntry2)] = None

  /** @return The data entries that still need to be enqueued. */
  def shutdown(): Map[Long, Option[DataEntry2]] = Map()
