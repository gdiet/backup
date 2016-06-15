package net.diet_rich.dedupfs.explorer.driver.dedup

import java.io.{IOException, InputStream}
import javafx.beans.property.{SimpleLongProperty, SimpleStringProperty}
import javafx.beans.value.{ObservableLongValue, ObservableStringValue}

import net.diet_rich.dedupfs._
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.{FilesTableItem, NameContainer}
import net.diet_rich.dedupfs.metadata.TreeEntry

class DedupFilesTableItem(repository: Repository.Any, treeEntry: TreeEntry) extends FilesTableItem {
  override def name: NameContainer = NameContainer(treeEntry.name) // TODO add vetoer
  override def canWrite: Boolean = false // TODO
  override def asFilesPaneDirectory: Option[FilesPaneDirectory] = if (isDirectory) Some(new DedupFilesPaneDirectory(repository, treeEntry)) else None
  override def execute(): Unit = ???
  override def size: ObservableLongValue = new SimpleLongProperty(treeEntry.data flatMap repository.metaBackend.sizeOf getOrElse 0L)
  override def isDirectory: Boolean = treeEntry.data.isEmpty
  override def time: ObservableLongValue = new SimpleLongProperty(treeEntry.changed getOrElse 0L)
  override def image: ObservableStringValue = new SimpleStringProperty(if (isDirectory) "image.folder" else "image.file")
  override def inputStream: InputStream = treeEntry.data map (repository.read(_).asInputStream) getOrElse (throw new IOException(s"$toString has no data"))
  override def toString = s"$schema item ${treeEntry.name}"
}
