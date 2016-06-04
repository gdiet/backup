package net.diet_rich.dedupfs.explorer

import java.io.{File, IOException}
import javafx.beans.property._
import javafx.scene.control.Alert
import javafx.scene.control.Alert.AlertType

import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.{FilesTableItem, NameContainer}

case class PhysicalFilesPaneDirectory(file: File) extends FilesPaneDirectory {
  override def url: String = s"file://$file"
  override def list: Seq[FilesTableItem] = file.listFiles.toSeq.map(new PhysicalFileTableItem(_))
  override def up: PhysicalFilesPaneDirectory = if (file.getParentFile == null) this else PhysicalFilesPaneDirectory(file.getParentFile)
}

class PhysicalFileTableItem(private var file: File) extends FilesTableItem {
  override val name: NameContainer = NameContainer(file.getName, renameTo)
  override val size: LongProperty = new SimpleLongProperty(file.length)
  override val time: LongProperty = new SimpleLongProperty(file.lastModified)
  override val image: String = if (file.isDirectory) "image.folder" else "image.file"
  override def isDirectory: Boolean = file.isDirectory
  override def isEditable: Boolean = file.canWrite
  override def execute(): Unit =
    try {java.awt.Desktop.getDesktop open file}
    catch {
      case e: IOException =>
        val alert = new Alert(AlertType.INFORMATION)
        alert setTitle conf.getString("dialog.cannotOpen.title")
        alert setHeaderText null
        alert setContentText conf.getString("dialog.cannotOpen.text").format(file)
        alert showAndWait()
    }
  override def asFilesPaneItem: Option[FilesPaneDirectory] = if (file.isDirectory) Some(PhysicalFilesPaneDirectory(file)) else None

  private def renameTo(newName: String): Boolean =
    newName.contains('/') || newName.contains('\\') || {
      val newFile = new File(file.getParentFile, newName)
      if (file renameTo newFile) {
        file = newFile
        false
      } else true
    }
}
