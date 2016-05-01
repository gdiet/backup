package net.diet_rich.dedupfs.commander

import javafx.beans.property.{LongProperty, Property}
import javafx.scene.image.Image

trait FilesTableItem {
  def name: Property[String]
  def size: LongProperty
  def icon: Property[Image]
  def isDirectory: Boolean
  def isEditable: Boolean
}
