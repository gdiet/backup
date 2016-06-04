package net.diet_rich.dedupfs.explorer.filesPane

trait FilesPaneDirectory {
  def list: Seq[filesTable.FilesTableItem]
  def up: FilesPaneDirectory
  def url: String
}
