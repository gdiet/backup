// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import net.diet_rich.dedup.database.TreeEntry
import net.diet_rich.dedup.database.TreeEntryID
import net.diet_rich.util.CallLogging
import java.io.File
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.dedup.database.Path

trait FileSystem {
  def entry(path: String): Option[TreeEntry]
  def path(id: TreeEntryID): Option[String]
  def children(id: TreeEntryID): Seq[TreeEntry]
  def child(id: TreeEntryID, childName: String): Option[TreeEntry]
}

object FileSystem {
  def apply(repositoryPath: String, writeEnabled: Boolean, deflate: Boolean): Either[Error, FileSystem] = try {
    val repositoryDir = new File(repositoryPath)
    val repository = if (repositoryDir isDirectory) Right(new Repository(repositoryDir, !writeEnabled))
      else Left(s"Repository '$repositoryPath' is not a directory.")
    repository.right map (new DedupFileSystem(_, backupDbOnShutdown = writeEnabled))
  } catch { case e: Throwable => Left(e getMessage) }
}

class DedupFileSystem(repository: Repository, backupDbOnShutdown: Boolean) extends FileSystem with CallLogging {
  sys.addShutdownHook {
    info("repository shutdown") { repository.shutdown(backupDbOnShutdown) }
  }
  
  def entry(path: String): Option[TreeEntry] = debug(s"entry(path: $path)") { repository.fs entry Path(path) }
  def path(id: TreeEntryID): Option[String] = debug(s"path(id: $id)") { repository.fs path id map (_.value) }
  def children(id: TreeEntryID): Seq[TreeEntry] = debug(s"children(id: $id)") { repository.fs children id }
  def child(id: TreeEntryID, childName: String): Option[TreeEntry] = debug(s"child(id: $id, childName: $childName)") { repository.fs child(id, childName) }
}
