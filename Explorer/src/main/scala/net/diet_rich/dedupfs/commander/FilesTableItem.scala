package net.diet_rich.dedupfs.commander

import java.io.File
import javafx.beans.property._
import javafx.beans.value.{ObservableValue, WritableValue}

trait FilesTableItem {
  def name: NameContainer
  def size: LongProperty
  def image: String
  def isDirectory: Boolean
  def isEditable: Boolean
  def open(): Unit
  def asFilesPaneItem: FilesPaneItem
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
  override val image: String = if (file.isDirectory) "image.folder" else "image.file"
  override def isDirectory: Boolean = file.isDirectory
  override def isEditable: Boolean = file.canWrite
  override def open(): Unit = java.awt.Desktop.getDesktop open file
  override def asFilesPaneItem: FilesPaneItem = PhysicalFilesPaneItem(file)

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
