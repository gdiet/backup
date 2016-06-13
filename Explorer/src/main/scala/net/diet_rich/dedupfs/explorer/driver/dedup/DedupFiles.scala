package net.diet_rich.dedupfs.explorer.driver.dedup

import java.io.File

import net.diet_rich.dedupfs.{Repository, FileSystem}
import net.diet_rich.dedupfs.explorer.filesPane.FileSystemRegistry

object DedupFiles {
  def registerIn(repository: Repository.Any, registry: FileSystemRegistry): FileSystemRegistry =
    registry withScheme("dup", { path =>
      repository.metaBackend.entry(path).headOption
      None
//      val file = new File(path)
//      if (file.isDirectory) Some(PhysicalFilesPaneDirectory(file)) else None
    })
}
