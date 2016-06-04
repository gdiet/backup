package net.diet_rich.dedupfs.explorer.filesPane.filesTable

import javafx.beans.value.{ObservableLongValue, ObservableStringValue}

import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory

trait FilesTableItem {
  // TODO make all observable values and observe them in the GUI
  def name: NameContainer // writable to enable renaming
  def size: ObservableLongValue
  def time: ObservableLongValue
  def image: ObservableStringValue
  def isDirectory: Boolean
  def isEditable: Boolean
  def execute(): Unit
  def asFilesPaneItem: Option[FilesPaneDirectory]
}
