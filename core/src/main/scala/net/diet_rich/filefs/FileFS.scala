package net.diet_rich.filefs

import java.io.{File => JFile}

import net.diet_rich.fs._
import net.diet_rich.util._

class FileFS extends FileSystemTrait { // FIXME make this an object

  override def roots: FSResult[Seq[Node]] = Good(JFile.listRoots().toSeq map Dir.apply)

  def entryFor(file: JFile): FSResult[Node] =
    if (file.isFile) Good(File(file))
    else if (file.isDirectory) Good(Dir(file))
    else NoSuchFile(s"$file does not exist")

  override def entryFor(path: String): FSResult[Node] = entryFor(new JFile(path))

  trait Node extends NodeTrait { val file: JFile }

  case class Dir(file: JFile) extends Node with DirTrait[Node] with AnyDirWrite[Node] {
    override def children = Good(file.listFiles().toSeq map Dir.apply)
    override def moveHere(node: Node) = {
      val target = new JFile(file, node.file.getName)
      if (node.file.renameTo(target)) entryFor(target)
      else OtherProblem(s"moving $node to $this failed.")
    }
    override def copyHere(node: AnyNode) = ???
  }

  case class File(file: JFile) extends Node with FileTrait with AnyFileWrite {
    override def lastModified = Good(file.lastModified)
    override def setLastModified(time: Long) = if (file.setLastModified(time)) Good(Unit) else Bad(OtherProblem(s"setLastModified failed for $file"))
  }
}

object FileFS extends App {
  val fs1 = new FileFS
  val fs2 = new FileFS

  val t1 = fs1.entryFor("e:/test1").good.asInstanceOf[fs1.Dir] // FIXME make .asDir and .asFile methods of anyNode
  val t2 = fs1.entryFor("e:/test2").good
  val t3 = fs2.entryFor("e:/test3").good
  val t4 = fs2.entryFor("e:/test4").good
  t1.moveHere(t2)
  t1.copyHere(t3)
}
