package net.diet_rich.fs

trait FileSystemTrait { self =>
  def roots: FSResult[Seq[NodeTrait]]
  def entryFor(path: String): FSResult[NodeTrait]

  trait NodeTrait extends AnyNode { def fileSystem: FileSystemTrait = self }
  trait DirTrait[Node <: NodeTrait] extends AnyDir[Node] with NodeTrait
  trait FileTrait extends AnyFile with NodeTrait
}

trait AnyNode {
  def fileSystem: FileSystemTrait
}

trait AnyNodeWrite[Node <: AnyNode] extends AnyNode {
  def delete(): FSResult[Unit]
  def renameTo(newName: String): FSResult[Node]
}

trait AnyDir[Node <: AnyNode] extends AnyNode {
  def children: FSResult[Seq[Node]]
}

trait AnyDirWrite[Node <: AnyNode] extends AnyDir[Node] {
  def moveHere(node: Node): FSResult[Node]
  def copyHere(node: AnyNode): FSResult[Node]
}

trait AnyFile extends AnyNode {
  def lastModified: FSResult[Long]
}

trait AnyFileWrite extends AnyFile {
  def setLastModified(time: Long): FSResult[Unit]
}

trait FSProblem {
  def description: String
  override def toString = description
}

case class NoSuchFile(description: String) extends FSProblem
case class OtherProblem(description: String) extends FSProblem
