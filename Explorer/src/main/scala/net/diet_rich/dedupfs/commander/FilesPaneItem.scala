package net.diet_rich.dedupfs.commander

import java.io.File

trait FilesPaneItem {
  def list: Seq[FilesTableItem]
  def up: FilesPaneItem
  def url: String
}

case class PhysicalFilesPaneItem(file: File) extends FilesPaneItem {
  override def url: String = s"file://$file"
  override def list: Seq[FilesTableItem] = file.listFiles.toSeq.map(FilesTableItem(_))
  override def up: PhysicalFilesPaneItem = if (file.getParentFile == null) this else PhysicalFilesPaneItem(file.getParentFile)
}
