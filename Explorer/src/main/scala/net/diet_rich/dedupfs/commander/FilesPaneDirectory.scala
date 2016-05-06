package net.diet_rich.dedupfs.commander

import java.io.File

trait FilesPaneDirectory {
  def list: Seq[FilesTableItem]
  def up: FilesPaneDirectory
  def url: String
}

case class PhysicalFilesPaneDirectory(file: File) extends FilesPaneDirectory {
  override def url: String = s"file://$file"
  override def list: Seq[FilesTableItem] = file.listFiles.toSeq.map(FilesTableItem(_))
  override def up: PhysicalFilesPaneDirectory = if (file.getParentFile == null) this else PhysicalFilesPaneDirectory(file.getParentFile)
}

object FileSystemRegistry {
  private var fileSystems = Map[String, String => Option[FilesPaneDirectory]]()
  def add(prefix: String, factory: String => Option[FilesPaneDirectory]): Unit = synchronized {
    require(!fileSystems.contains(prefix), s"File system for prefix $prefix already registered")
    fileSystems += prefix -> factory
  }
  def remove(prefix: String): Unit = synchronized { fileSystems -= prefix }
  def get(url: String): Option[FilesPaneDirectory] = url.split("://", 2) match {
    case Array(prefix, file) => fileSystems get prefix flatMap (_(file))
    case _ => None
  }

  add("file", { path =>
    val file = new File(path)
    if (file.isDirectory) Some(PhysicalFilesPaneDirectory(file)) else None
  })
}
