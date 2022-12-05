package dedup
package server

class Backend(settings: Settings) extends util.ClassLogging:
  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val db = dedup.db.DB(dedup.db.H2.connection(settings.dbDir, settings.readonly))
  private val handles = Handles()

  /** @return The size of the file. If the cached size if any or the logical size of the file's data entry. */
  def size(fileEntry: FileEntry): Long =
    handles.cachedSize(fileEntry.id).getOrElse(db.logicalSize(fileEntry.dataId))
