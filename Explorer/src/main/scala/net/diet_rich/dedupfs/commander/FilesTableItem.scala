package net.diet_rich.dedupfs.commander

import java.io.{File, IOException}
import javafx.beans.property._
import javafx.beans.value.{ObservableValue, WritableValue}
import javafx.scene.control.Alert
import javafx.scene.control.Alert.AlertType

trait FilesTableItem {
  // TODO make all observable values and observe them in the GUI
  def name: NameContainer
  def size: LongProperty
  def time: LongProperty
  def image: String
  def isDirectory: Boolean
  def isEditable: Boolean
  def open(): Unit
  def asFilesPaneItem: Option[FilesPaneDirectory]
}

trait NameContainer extends ObservableValue[String] with WritableValue[String]
object NameContainer {
  class VetoException(text: String) extends RuntimeException(text)

  def apply(initialValue: String, vetoer: String => Boolean = _ => true): NameContainer = new SimpleStringProperty(initialValue) with NameContainer {
    override def setValue(string: String): Unit =
      if (vetoer(string)) throw new VetoException(s"Vetoed changing $getValue to $string")
      else super.setValue(string)
  }
}

class PhysicalFileTableItem(private var file: File) extends FilesTableItem {
  override val name: NameContainer = NameContainer(file.getName, renameTo)
  override val size: LongProperty = new SimpleLongProperty(file.length)
  override val time: LongProperty = new SimpleLongProperty(file.lastModified)
  override val image: String = if (file.isDirectory) "image.folder" else "image.file"
  override def isDirectory: Boolean = file.isDirectory
  override def isEditable: Boolean = file.canWrite
  override def open(): Unit =
    try { java.awt.Desktop.getDesktop open file }
    catch { case e: IOException =>
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

object FilesTableItem {
  def apply(file: File): FilesTableItem = new PhysicalFileTableItem(file)
}
