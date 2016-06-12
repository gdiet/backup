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

  // FIXME duplicate code
  override def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit =
    files exists { fileToCopy =>
      val operationalState = fileToCopy match {
        case item: PhysicalFileTableItem =>
          if (item copyTo file) Success else Failure
        case item =>
          println(s"moving ${item.getClass} to a physical directory not implemented yet")
          Failure
      }
      onItemHandled(fileToCopy, operationalState) == Abort
    }

  override def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit =
    files exists { fileToMove =>
      val operationalState = fileToMove match {
        case item: PhysicalFileTableItem =>
          if (item moveTo file) Success else Failure
        case item =>
          println(s"moving ${item.getClass} to a physical directory not implemented yet")
          Failure
      }
      onItemHandled(fileToMove, operationalState) == Abort
    }

  override def toString = s"directory $file"
}
