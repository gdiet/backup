package net.diet_rich.dedupfs.explorer.driver.dedup

import net.diet_rich.dedupfs.Repository
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory.ItemHandler
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem
import net.diet_rich.dedupfs.metadata.TreeEntry

class DedupFilesPaneDirectory(repository: Repository.Any, treeEntry: TreeEntry) extends FilesPaneDirectory {
  override def list: Seq[FilesTableItem] = ???
  override def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit = ???
  override def url: String = ???
  override def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit = ???
  override def up: FilesPaneDirectory = ???
}
