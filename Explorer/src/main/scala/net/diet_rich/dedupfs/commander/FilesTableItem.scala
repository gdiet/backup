package net.diet_rich.dedupfs.commander

import java.io.File
import javafx.beans.property._
import javafx.scene.image.Image

trait FilesTableItem {
  def name: Property[String]
  def size: LongProperty
  def icon: Property[Image]
  def isDirectory: Boolean
  def isEditable: Boolean
}

case class PhysicalFileTableItem(file: File) extends FilesTableItem {
  override val name: Property[String] = new SimpleStringProperty(file.getName)
  override val size: LongProperty = new SimpleLongProperty(file.length)
  override val icon: Property[Image] = new SimpleObjectProperty[Image](if (file.isDirectory) imageFolder else imageFile)
  override def isDirectory: Boolean = file.isDirectory
  override def isEditable: Boolean = file.canWrite
}

object FilesTableItem {
  def apply(file: File): FilesTableItem = PhysicalFileTableItem(file)
}
