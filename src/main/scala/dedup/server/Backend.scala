package dedup
package server

object Backend:
  private val entryCount = new java.util.concurrent.atomic.AtomicLong()
  private val entriesSize = new java.util.concurrent.atomic.AtomicLong()
  def cacheLoad: Long = entriesSize.get() * entryCount.get()

class Backend(settings: Settings) extends AutoCloseable with util.ClassLogging:
  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val handles = Handles(settings.tempPath)

  override def close(): Unit =
    handles.shutdown() // FIXME handle the result
    lts.close()

  /** @return The size of the file. If the cached size if any or the logical size of the file's data entry. */
  def size(fileEntry: FileEntry): Long =
    handles.cachedSize(fileEntry.id).getOrElse(db.logicalSize(fileEntry.dataId))

  def truncate(id: Long, newSize: Long): Boolean =
    ???
