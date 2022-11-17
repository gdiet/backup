package dedup
package db

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Using.resource

object blacklist extends util.ClassLogging:

  /** @param dbDir Database directory
    * @param blacklistDir Directory containing files to add to the blacklist
    * @param deleteFiles If true, files in the `blacklistDir` are deleted when they have been taken over
    * @param dfsBlacklist Name of the base blacklist directory in the dedup file system, resolved against root
    * @param deleteCopies If true, mark deleted all blacklisted occurrences except for the original entries in `dfsBlacklist` */
  def apply(dbDir: File, blacklistDir: String, deleteFiles: Boolean, dfsBlacklist: String, deleteCopies: Boolean): Unit = withDb(dbDir, readonly = false) { db =>
    db.mkDir(root.id, dfsBlacklist).foreach(_ => log.info(s"Created blacklist directory DedupFS:/$dfsBlacklist"))
    db.child(root.id, dfsBlacklist) match
      case None                          => log.error(s"Can't run blacklisting - couldn't create DedupFS:/$dfsBlacklist.")
      case Some(_: FileEntry)            => log.error(s"Can't run blacklisting - DedupFS:/$dfsBlacklist is a file, not a directory.")
      case Some(blacklistRoot: DirEntry) =>
        log.info(s"Blacklisting now...")

        // Add external files to blacklist.
        val blacklistFolder = File(blacklistDir).getCanonicalFile
        val dateString = SimpleDateFormat("yyyy-MM-dd_HH-mm").format(Date())
        db.mkDir(blacklistRoot.id, dateString).foreach(externalFilesToInternalBlacklist(db, blacklistFolder, _, deleteFiles))

        // Process internal blacklist.
        processInternalBlacklist(db, dfsBlacklist, s"/${blacklistRoot.name}", blacklistRoot.id, deleteCopies)
        log.info(s"Finished blacklisting.")
  }

  def externalFilesToInternalBlacklist(db: Database, currentDir: File, dirId: Long, deleteFiles: Boolean): Unit =
    Option(currentDir.listFiles()).toSeq.flatten.foreach { file =>
      if file.isDirectory then
        db.mkDir(dirId, file.getName) match
          case None => problem("blacklist.create.dir", s"can't create internal blacklist directory for $file")
          case Some(childDirId) => externalFilesToInternalBlacklist(db, file, childDirId, deleteFiles)
        if !deleteFiles then {} else // needed like this to avoid compile problem
          if Option(file.listFiles).exists(_.isEmpty) then file.delete else
            log.warn(s"Blacklist directory not empty after processing it: $file")
      else
        val (size, hash) = resource(FileInputStream(file)) { stream =>
          val buffer = new Array[Byte](memChunk)
          val md = java.security.MessageDigest.getInstance(hashAlgorithm)
          val size = Iterator.continually(stream.read(buffer)).takeWhile(_ > 0)
            .tapEach(md.update(buffer, 0, _)).map(_.toLong).sum
          size -> md.digest()
        }
        val dataId = db.dataEntry(hash, size).getOrElse(
          DataId(db.nextId).tap(db.insertDataEntry(_, 1, size, 0, 0, hash))
        )
        ensure("blacklist.create.file", db.mkFile(dirId, file.getName, Time(file.lastModified), dataId).isDefined,
          s"can't create internal blacklist file for $file")
        if deleteFiles && file.delete then
          log.info(s"Moved to DedupFS blacklist: $file")
        else
          log.info(s"Copied to DedupFS blacklist: $file")
    }

  def processInternalBlacklist(db: Database, dfsBlacklist: String, parentPath: String, parentId: Long, deleteCopies: Boolean): Unit =
    db.children(parentId).foreach {
      case dir: DirEntry =>
        processInternalBlacklist(db, dfsBlacklist, s"$parentPath/${dir.name}", dir.id, deleteCopies)
      case file: FileEntry =>
        if db.storageSize(file.dataId) > 0 then
          log.info(s"Blacklisting $parentPath/${file.name}")
          db.removeStorageAllocation(file.dataId)
        if deleteCopies then
          val copies = db.entriesFor(file.dataId).filterNot(_.id == file.id)
          val filteredCopies = copies
            .map(entry => (entry.id, db.pathOf(entry.id)))
            .filterNot(_._2.startsWith(s"/$dfsBlacklist/"))
          filteredCopies.foreach { (id, path) =>
            log.info(s"Deleting copy of entry: $path")
            if db.deleteChildless(id)
            then log.info(s"Marked deleted file '$path' .. ${readableBytes(db.dataSize(file.dataId))}")
            else log.warn(s"Could not delete file with children: '$path'")
          }
    }
