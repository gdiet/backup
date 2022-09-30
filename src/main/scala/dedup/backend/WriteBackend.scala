package dedup
package backend

import dedup.db.WriteDatabase
import dedup.server.Settings

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
final class WriteBackend(settings: Settings, db: WriteDatabase) extends ReadBackend(settings, db):

  // Write the cache before closing
  override def close(): Unit = sync { ???; super.close() }

  // Return the cached size if any
  override def size(fileEntry: FileEntry): Long = ???
