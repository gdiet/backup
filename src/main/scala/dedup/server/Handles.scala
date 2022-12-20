package dedup
package server

// FIXME implementation missing
// FIXME documentation missing
final class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = None

  /** Create the virtual file handle if missing and increment the handle count. */
  def open(fileId: Long, dataId: DataId): Unit = ()
