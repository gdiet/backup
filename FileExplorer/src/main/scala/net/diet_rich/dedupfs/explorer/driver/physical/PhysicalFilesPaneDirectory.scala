package net.diet_rich.dedupfs.explorer.driver.physical

import java.io.{File, IOException}

import net.diet_rich.dedupfs.explorer.filesPane._
import net.diet_rich.dedupfs.explorer.filesPane.FilesPaneDirectory.ItemHandler
import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem
import org.apache.commons.io.FileUtils

case class PhysicalFilesPaneDirectory(file: File) extends FilesPaneDirectory {
  require(file.isDirectory, s"File is not a directory: $file")

  override def url: String = s"$schema://$file"
  override def list: Seq[PhysicalFilesTableItem] = file.listFiles.toSeq.map(new PhysicalFilesTableItem(_))
  override def up: PhysicalFilesPaneDirectory = if (file.getParentFile == null) this else PhysicalFilesPaneDirectory(file.getParentFile)

  private def actionHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler, actionName: String, action: (PhysicalFilesTableItem, File) => Boolean): Unit =
    files exists { fileToProcess =>
      val operationalState = fileToProcess match {
        case item: PhysicalFilesTableItem =>
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

  override def getOrCreateChildDirectory(name: String): Option[FilesPaneDirectory] = {
    val child = new File(file, name)
    if (child.isDirectory || child.mkdir()) Some(PhysicalFilesPaneDirectory(child)) else None
  }

  override protected val nativeCopyHere2: PartialFunction[(FilesTableItem, ItemHandler), OperationalImplication] = {
    case (item: PhysicalFilesTableItem, onItemHandled) =>
      onItemHandled(item, item.copyTo2(file))
    case (item: FilesTableItem, onItemHandled) if !item.isDirectory =>
      val target = new File(file, item.name.getValue)
      if (target.exists()) onItemHandled(item, Failure)
      else try {
        FileUtils.copyInputStreamToFile(item.inputStream, target)
        onItemHandled(item, Success)
      } catch {
        case e: IOException =>
          println(s"could not copy data into $target")
          onItemHandled(item, Failure)
      }
  }

  override def toString = s"$schema folder $file"
}
