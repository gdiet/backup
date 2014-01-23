// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.CallLogging
import net.diet_rich.util.io.ByteSource
import java.io.File

object DedupFileSystem {
  def apply(repositoryPath: String, writeEnabled: Boolean, deflate: Boolean): Either[Error, DedupFileSystem] = try {
    val repositoryDir = new File(repositoryPath)
    val repository = if (repositoryDir isDirectory) Right(new Repository(repositoryDir, !writeEnabled, false))
      else Left(s"Repository '$repositoryPath' is not a directory.")
    repository.right map (new DedupFileSystem(_, backupDbOnShutdown = writeEnabled))
  } catch { case e: Throwable => Left(e getMessage) }
}

class DedupFileSystem(repository: Repository, backupDbOnShutdown: Boolean) extends CallLogging {
  sys.addShutdownHook {
    info("repository shutdown") { repository.shutdown(backupDbOnShutdown) }
  }
  
  def entry(path: String): Option[TreeEntry] = debug(s"entry(path: $path)") { repository.fs entry Path(path) }
  def path(id: TreeEntryID): Option[String] = debug(s"path(id: $id)") { repository.fs path id map (_.value) }
  def children(id: TreeEntryID): Seq[TreeEntry] = debug(s"children(id: $id)") { repository.fs children id }
  def child(id: TreeEntryID, childName: String): Option[TreeEntry] = debug(s"child(id: $id, childName: $childName)") { repository.fs child(id, childName) }
  def dataEntry(dataid: DataEntryID): DataEntry = debug(s"dataEntry(dataid: $dataid)") { repository.fs.dataEntry(dataid) }
  def bytes(dataid: DataEntryID, method: Method) = debug(s"bytes(dataid: $dataid, method: $method)") { repository.fs.read(dataid, method) }
  def markDeleted(id: TreeEntryID): Boolean = debug(s"markDeleted(id: $id)") { repository.fs markDeleted id }
  def moveRename(id: TreeEntryID, newName: String, newParent: TreeEntryID): Boolean = debug(s"moveRename(id: $id, newName: $newName, newParent: $newParent)") { repository.fs changePath (id, newName, newParent) }
}
