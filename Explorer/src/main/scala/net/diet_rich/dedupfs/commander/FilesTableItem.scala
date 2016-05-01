package net.diet_rich.dedupfs.commander

import javafx.beans.property.Property
import javafx.scene.image.Image

trait FilesTableItem {
  def name: Property[String]
  def size: Property[String]
  def icon: Property[Image]
  def isEditable: Boolean
}
