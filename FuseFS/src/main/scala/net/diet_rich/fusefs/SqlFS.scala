package net.diet_rich.fusefs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.scalafs._
import net.diet_rich.util.fs._
import net.diet_rich.util.sql.ConnectionFactory

class SqlFS extends FileSystem {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  private val meta = new H2MetaBackend
  // FIXME remove
  val root: Dir = getNode("/").get.asInstanceOf[Dir]
  println(root.mkDir("hallo"))
  println(root.mkDir("hallo"))
  println(root.mkDir("welt"))
  val welt: Dir = getNode("/welt").get.asInstanceOf[Dir]
  println(welt.mkDir("hello"))
  println(root.list.toList)

  private def fs[T](t: T): T = synchronized(t)

  private def nodeFor(entry: meta.TreeEntry): Node = {
    def rename(path: String): RenameResult = {
      FileSystem.pathElements(path).getOrElse(Seq()).reverse match {
        case List() => TargetExists
        case newName :: elements =>
          meta.entry(elements.reverse) match {
            case None => TargetParentDoesNotExist
            case Some(newParent) =>
              if (newParent.data.nonEmpty) TargetParentNotADirectory
              else meta.rename(entry.id, newName, newParent.id)
          }
      }
    }
    if (entry.isDir) new Dir {
      override def delete(): DeleteResult = fs(meta.delete(entry.id))
      override def list: Seq[Node] = fs(meta.children(entry.id).map(nodeFor))
      override def mkDir(child: String): Boolean = fs(meta.addNewDir(entry.id, child).isDefined)
      override def name: String = fs(entry.name)
      override def renameTo(path: String): RenameResult = fs(rename(path))
      override def toString: String = fs(s"Dir '$name': $entry")
    } else new File {
      override def delete(): DeleteResult = fs(meta.delete(entry.id))
      override def name: String = fs(entry.name)
      override def renameTo(path: String): RenameResult = fs(rename(path))
      override def size: Long = fs(0) // FIXME
      override def toString: String = fs(s"File '$name': $entry")
    }
  }

  override def getNode(path: String): Option[Node] = fs(FileSystem.pathElements(path).flatMap(meta.entry).map(nodeFor))
}
