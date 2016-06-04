package net.diet_rich.dedupfs.explorer.filesPane.filesTable

import javafx.beans.property.{StringProperty, LongProperty}

import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory

trait FilesTableItem {
  // TODO make all observable values and observe them in the GUI
  def name: NameContainer
  def size: LongProperty
  def time: LongProperty
  def image: StringProperty
  def isDirectory: Boolean
  def isEditable: Boolean
  def execute(): Unit
  def asFilesPaneItem: Option[FilesPaneDirectory]
}

