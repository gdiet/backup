package net.diet_rich.dedupfs.explorer.filesPane

import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem

trait FilesPaneDirectory {

  import FilesPaneDirectory._

  def list: Seq[filesTable.FilesTableItem]
  def up: FilesPaneDirectory
  def url: String
  def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit
  def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit
}

object FilesPaneDirectory {
  type ItemHandler = (FilesTableItem, OperationalState) => OperationalImplication
}

sealed trait OperationalState

object Success extends OperationalState

object Failure extends OperationalState

sealed trait OperationalImplication

object Continue extends OperationalImplication

object Abort extends OperationalImplication
