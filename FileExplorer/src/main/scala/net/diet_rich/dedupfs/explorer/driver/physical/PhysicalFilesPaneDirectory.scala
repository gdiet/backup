package net.diet_rich.dedupfs.explorer.driver.physical

import java.io.File

import net.diet_rich.dedupfs.explorer.filesPane.{Failure, Abort, Success, FilesPaneDirectory}
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory.ItemHandler
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem

case class PhysicalFilesPaneDirectory(file: File) extends FilesPaneDirectory {
  require(file.isDirectory, s"File is not a directory: $file")

  override def url: String = s"file://$file"
  override def list: Seq[PhysicalFileTableItem] = file.listFiles.toSeq.map(new PhysicalFileTableItem(_))
  override def up: PhysicalFilesPaneDirectory = if (file.getParentFile == null) this else PhysicalFilesPaneDirectory(file.getParentFile)

  private def actionHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler, actionName: String, action: (PhysicalFileTableItem, File) => Boolean): Unit =
    files exists { fileToProcess =>
      val operationalState = fileToProcess match {
        case item: PhysicalFileTableItem =>
          if (action(item, file)) Success else Failure
        case item =>
          println(s"$actionName for ${item.getClass} to a physical directory not implemented yet")
          Failure
      }
      onItemHandled(fileToProcess, operationalState) == Abort
    }
  override def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit =
    actionHere(files, onItemHandled, "copy", _ copyTo _)
  override def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit =
    actionHere(files, onItemHandled, "move", _ moveTo _)

  override def toString = s"directory $file"
}
