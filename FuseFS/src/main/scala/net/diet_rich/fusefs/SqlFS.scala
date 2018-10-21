package net.diet_rich.fusefs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.util.fs._
import net.diet_rich.util.sql.ConnectionFactory

object SqlFS {
  val separator = "/"
  val rootPath  = "/"

  def pathElements(path: String): Option[List[String]] = {
    if (!path.startsWith(separator)) None else
    if (path == rootPath) Some(List()) else
      Some(path.split(separator).toList.drop(1))
  }

  def splitParentPath(path: String): Option[(String, String)] =
    if (!path.startsWith(separator) || path == rootPath) None else {
      Some(path.lastIndexOf('/') match {
        case 0 => ("/", path.drop(1))
        case i => (path.take(i), path.drop(i+1))
      })
    }
}
class SqlFS {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  protected val meta: H2MetaBackend = new H2MetaBackend
  private val unused: Unit = { // FIXME remove
    val root: Dir = getNode("/").get.asInstanceOf[Dir]
    println(root.mkDir("hallo"))
    println(root.mkDir("hallo"))
    println(root.mkDir("welt"))
    val welt: Dir = getNode("/welt").get.asInstanceOf[Dir]
    println(welt.mkDir("hello"))
    println(root.list.toList)
  }

  private def fs[T](t: T): T = synchronized(t)

  sealed trait Node {
    protected def entry: meta.TreeEntry
    final def name: String = fs(entry.name)
    final def renameTo(path: String): RenameResult = fs {
      SqlFS.pathElements(path).getOrElse(Seq()).reverse match {
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
  }

  class Dir(val entry: meta.TreeEntry) extends Node {
    def delete(): DeleteResult = fs(meta.delete(entry.id))
    def list: Seq[Node] = fs(meta.children(entry.id).map(nodeFor))
    def mkDir(child: String): Boolean = fs(meta.addNewDir(entry.id, child).isDefined)
    override def toString: String = fs(s"Dir '$name': $entry")
  }

  class File(val entry: meta.TreeEntry) extends Node {
    def delete(): DeleteResult = fs(meta.delete(entry.id))
    def size: Long = fs(0) // FIXME
    override def toString: String = fs(s"File '$name': $entry")
  }

  private def nodeFor(entry: meta.TreeEntry): Node = {
    if (entry.isDir) new Dir(entry) else new File(entry)
  }

  def getNode(path: String): Option[Node] = fs(SqlFS.pathElements(path).flatMap(meta.entry).map(nodeFor))
}
