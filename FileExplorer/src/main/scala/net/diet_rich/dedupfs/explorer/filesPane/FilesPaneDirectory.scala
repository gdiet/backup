package net.diet_rich.dedupfs.explorer.filesPane

import net.diet_rich.dedupfs.explorer.filesPane.filesTable.FilesTableItem

trait FilesPaneDirectory {
  import FilesPaneDirectory._

  def list: Seq[filesTable.FilesTableItem]
  def up: FilesPaneDirectory
  def url: String
  def copyHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit
  def moveHere(files: Seq[FilesTableItem], onItemHandled: ItemHandler): Unit

  final def copyHere2(files: Seq[FilesTableItem], onItemHandled: ItemHandler): OperationalImplication =
    if (files.exists(file => copyHere2(file, onItemHandled) == Abort)) Abort else Continue

  final def copyHere2(file: FilesTableItem, onItemHandled: ItemHandler): OperationalImplication =
    nativeCopyHere2.applyOrElse((file, onItemHandled), directoryCopyHere2)

  private val directoryCopyHere2: Function[(FilesTableItem, ItemHandler), OperationalImplication] = {
    case (file, onItemHandled) if file.isDirectory =>
      file.asFilesPaneDirectory.fold {
        println(s"could not create directory for $file") // TODO Failure details
        onItemHandled(file, Failure)
      }{ sourceDirectory =>
        getOrCreateChildDirectory(file.name.getValue).fold {
          println(s"could not create directory named ${file.name.getValue} in $this")
          onItemHandled(file, Failure)
        }{ targetDirectory =>
          targetDirectory.copyHere2(sourceDirectory.list, onItemHandled)
        }
      }
    case (file, onItemHandled) =>
      println(s"copy operation not supported for $file to $this")
      onItemHandled(file, Failure)
  }

  def getOrCreateChildDirectory(name: String): Option[FilesPaneDirectory]
  protected val nativeCopyHere2: PartialFunction[(FilesTableItem, ItemHandler), OperationalImplication]
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
