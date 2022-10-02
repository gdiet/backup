package dedup
package backend

import dedup.db.{ReadDatabase, WriteDatabase}
import dedup.server.Settings
import dedup.{DirEntry, FileEntry, TreeEntry}

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
class ReadBackend(settings: Settings, db: ReadDatabase) extends Backend:

  // *** Tree and meta data operations ***
  
  override def size(fileEntry: FileEntry): Long = sync { db.logicalSize(fileEntry.dataId) }
  
  override final def children(parentId: Long): Seq[TreeEntry] = sync { db.children(parentId) }

  override final def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => sync { db.child(dir.id, name) }
      case _ => None
    }



