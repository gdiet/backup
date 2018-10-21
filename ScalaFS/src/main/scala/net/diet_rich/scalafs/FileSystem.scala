package net.diet_rich.scalafs
import net.diet_rich.util.fs.{DeleteResult, RenameResult}

sealed trait Node {
  def delete(): DeleteResult
  def name: String
  def renameTo(path: String): RenameResult
}

trait Dir extends Node {
  def list: Seq[Node]
  def mkDir(child: String): Boolean
}

trait File extends Node {
  def size: Long
}
