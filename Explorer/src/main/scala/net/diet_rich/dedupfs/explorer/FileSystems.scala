package net.diet_rich.dedupfs.explorer

import java.io.File

import ExplorerFile.AFile

trait FileSystems {
  def fileFor(uri: String): Option[AFile]
  final def directoryFor(uri: String): Option[AFile] = fileFor(uri).filter(_.isDirectory)
}

object FileSystems extends FileSystems {
  def fileFor(uri: String): Option[AFile] = {
    val Array(protocol, path) = uri.split("\\:\\/\\/", 2)
    protocol match {
      case "file" =>
        val file = new File(path)
        if (file.exists()) Some(PhysicalExplorerFile(file)) else None
      case _ => None
    }
  }
}
