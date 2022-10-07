package dedup
package backend

import dedup.db.WriteDatabase
import dedup.server.{DataSink, Settings}

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
final class WriteBackend(settings: Settings, db: WriteDatabase) extends ReadBackend(settings, db):

  // TODO Write the cache before closing
  override def shutdown(): Unit = sync {
    db.shutdownCompact()
    super.shutdown()
  }

  // TODO Return the cached size if any
  override def size(fileEntry: FileEntry): Long = super.size(fileEntry)

  override def mkDir(parentId: Long, name: String): Option[Long] = sync { db.mkDir(parentId, name) }

  override def setTime(id: Long, newTime: Long): Unit = sync { db.setTime(id, newTime) }

  override def deleteChildless(entry: TreeEntry): Boolean = sync {
    if db.children(entry.id).nonEmpty then false else { db.delete(entry.id); true }
  }

  override def release(fileId: Long): Boolean =
    sync { releaseInternal(fileId) } match
      case None => false
      case Some(count -> dataId) =>
        if count < 1 then log.info(s"dataId $dataId: write-through not implemented") // TODO implement write-through
        true

// TODO implement
//  override def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] = ???
