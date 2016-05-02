package net.diet_rich.dedupfs.commander

import java.io.File
import javafx.beans.property._
import javafx.beans.value.{ObservableValue, WritableValue}
import javafx.scene.image.Image

trait FilesTableItem {
  def name: NameContainer
  def size: LongProperty
  def icon: Property[Image]
  def isDirectory: Boolean
  def isEditable: Boolean
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
  override val icon: Property[Image] = new SimpleObjectProperty[Image](if (file.isDirectory) imageFolder else imageFile)
  override def isDirectory: Boolean = file.isDirectory
  override def isEditable: Boolean = file.canWrite

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
