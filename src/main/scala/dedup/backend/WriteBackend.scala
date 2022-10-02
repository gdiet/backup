package dedup
package backend

import dedup.db.WriteDatabase
import dedup.server.Settings

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
final class WriteBackend(settings: Settings, db: WriteDatabase) extends ReadBackend(settings, db):

  // TODO Write the cache before closing
  def shutdown(): Unit = sync { db.shutdownCompact() }

  // TODO Return the cached size if any
  override def size(fileEntry: FileEntry): Long = super.size(fileEntry)

  override def mkDir(parentId: Long, name: String): Option[Long] = sync { db.mkDir(parentId, name) }
