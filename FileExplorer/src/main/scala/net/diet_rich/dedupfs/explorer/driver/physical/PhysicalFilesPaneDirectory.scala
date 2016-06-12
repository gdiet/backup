package net.diet_rich.dedupfs.explorer.driver.physical

import java.io.File

import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory.ItemHandler
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem

case class PhysicalFilesPaneDirectory(file: File) extends FilesPaneDirectory {
  override def url: String = s"file://$file"
  override def list: Seq[PhysicalFileTableItem] = file.listFiles.toSeq.map(new PhysicalFileTableItem(_))
  override def up: PhysicalFilesPaneDirectory = if (file.getParentFile == null) this else PhysicalFilesPaneDirectory(file.getParentFile)
  override def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit = ???
  override def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit = ???
}
