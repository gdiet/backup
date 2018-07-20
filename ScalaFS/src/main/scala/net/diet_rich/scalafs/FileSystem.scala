package net.diet_rich.scalafs

trait FileSystem {
  def getNode(path: String): Option[Node]
}

sealed trait Node {
  def name: String
}

trait Dir extends Node {
  def list: Seq[Node]
}

trait File extends Node {
  def size: Long
}
