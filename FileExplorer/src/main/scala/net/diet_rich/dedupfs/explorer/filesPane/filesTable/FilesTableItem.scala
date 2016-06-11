package net.diet_rich.dedupfs.explorer.filesPane.filesTable

import javafx.beans.value.{ObservableLongValue, ObservableStringValue}

import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory

trait FilesTableItem {
  def name: NameContainer // writable to enable renaming
  def size: ObservableLongValue
  def time: ObservableLongValue
  def image: ObservableStringValue
  def isDirectory: Boolean // assumed to be immutable
  def canWrite: Boolean // assumed to be immutable
  def execute(): Unit
  def asFilesPaneItem: Option[FilesPaneDirectory]
}
