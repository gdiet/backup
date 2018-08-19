package net.diet_rich.scalafs

trait FileSystem {
  def getNode(path: String): Option[Node]
  def splitParentPath(path: String): Option[(String, String)]
}

sealed trait Node {
  def name: String
}

trait Dir extends Node {
  def list: Seq[Node]
  def mkDir(child: String): Boolean
}

trait File extends Node {
  def size: Long
}
