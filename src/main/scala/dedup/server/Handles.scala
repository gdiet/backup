package dedup
package server

// FIXME implementation missing
final class Handles(tempPath: java.nio.file.Path) extends util.ClassLogging:

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] = None
