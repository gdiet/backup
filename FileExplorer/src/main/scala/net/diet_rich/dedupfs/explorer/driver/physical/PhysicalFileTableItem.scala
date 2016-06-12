package net.diet_rich.dedupfs.explorer.driver.physical

import java.io.{File, IOException}
import javafx.beans.property.{LongProperty, SimpleLongProperty, SimpleStringProperty, StringProperty}
import javafx.scene.control.Alert
import javafx.scene.control.Alert.AlertType

import net.diet_rich.dedupfs.explorer.conf
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.{FilesTableItem, NameContainer}

class PhysicalFileTableItem(private var file: File) extends FilesTableItem {
  override val name: NameContainer = NameContainer(file.getName, renameTo)
  override val size: LongProperty = new SimpleLongProperty(file.length)
  override val time: LongProperty = new SimpleLongProperty(file.lastModified)
  override val image: StringProperty = new SimpleStringProperty(if (file.isDirectory) "image.folder" else "image.file")
  override def isDirectory: Boolean = file.isDirectory
  override def canWrite: Boolean = file.canWrite
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
  override def asFilesPaneItem: Option[PhysicalFilesPaneDirectory] = if (file.isDirectory) Some(PhysicalFilesPaneDirectory(file)) else None

  private def renameTo(newName: String): Boolean =
    newName.contains('/') || newName.contains('\\') || {
      val newFile = new File(file.getParentFile, newName)
      if (file renameTo newFile) {
        file = newFile
        false
      } else true
    }
}
