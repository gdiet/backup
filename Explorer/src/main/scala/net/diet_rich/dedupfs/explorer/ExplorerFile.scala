package net.diet_rich.dedupfs.explorer

import java.io.File

import ExplorerFile.AFile
import net.diet_rich.dedupfs.DedupFile

object ExplorerFile {
  type AFile = ExplorerFile[_]
}

trait SortableFile {
  def isDirectory: Boolean
  def size: Long
  def name: String
  def time: Long
}

case class FileNameContainer(name: String) extends SortableFile {
  override def isDirectory: Boolean = throw new NotImplementedError() // TODO better exception
  override def size: Long = throw new NotImplementedError()
  override def time: Long = throw new NotImplementedError()
}

trait ExplorerFile[FileType <: AFile] extends SortableFile {_: FileType =>
  def path: String
  def list: Seq[AFile]
  def parent: AFile
  def moveTo(other: FileType): Boolean

  // delete (DEL)
  // copyFrom or copyTo (F5)
  // open (double-click)
  // view (F3)
  // edit (F4)
  // acceptDrop or something
  // drag or something
}

case class DedupSystemFile(file: DedupFile) extends ExplorerFile[DedupSystemFile] {
  override def isDirectory: Boolean = file.isDirectory
  override def moveTo(other: DedupSystemFile): Boolean = ???
  override def size: Long = file.size
  override def name: String = file.name
  override def time: Long = file.lastModified
  override def list: Seq[AFile] = file.children.toSeq map DedupSystemFile
  override def path: String = s"dedup://${file.path}"
  override def parent: AFile = DedupSystemFile(file.parent)
}

case class PhysicalExplorerFile(file: File) extends ExplorerFile[PhysicalExplorerFile] {
  override def name = file.getName
  override def path = s"file://${file.getPath}"
  override def size = file.length()
  override def time = file.lastModified()
  override def isDirectory = file.isDirectory
  override def list = file.listFiles().toSeq map PhysicalExplorerFile
  override def parent = PhysicalExplorerFile(Option(file.getParentFile) getOrElse file)
  override def moveTo(other: PhysicalExplorerFile) = file.renameTo(other.file)
}
