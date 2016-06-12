package net.diet_rich.dedupfs.explorer.driver.physical

import java.io.File

import net.diet_rich.dedupfs.explorer.filesPane.FileSystemRegistry

object PhysicalFiles {
  def registerIn(registry: FileSystemRegistry): FileSystemRegistry =
    registry withScheme("file", { path =>
      val file = new File(path)
      if (file.isDirectory) Some(PhysicalFilesPaneDirectory(file)) else None
    })
}
