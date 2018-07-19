package net.diet_rich.scalafs

trait FileSystem {
  def getPath(path: String): Option[DirOrFile]
}

sealed trait DirOrFile

trait Dir extends DirOrFile

trait File extends DirOrFile {
  def size: Long
}
