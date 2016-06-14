package net.diet_rich.dedupfs.explorer.driver.dedup

import java.io.File

import net.diet_rich.dedupfs.{Repository, FileSystem}
import net.diet_rich.dedupfs.explorer.filesPane.FileSystemRegistry

object DedupFiles {
  def registerIn(repository: Repository.Any, registry: FileSystemRegistry): FileSystemRegistry =
    registry withSchema (schema, { path =>
      // TODO check whether is directory!
      repository.metaBackend.entry(path).headOption map (new DedupFilesPaneDirectory(repository, _))
    })
}
