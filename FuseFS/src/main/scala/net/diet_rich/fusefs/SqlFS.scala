package net.diet_rich.fusefs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.scalafs.{Dir, File, FileSystem, Node}
import net.diet_rich.util.sql.ConnectionFactory

class SqlFS extends FileSystem {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl(), H2.user, H2.password, H2.onShutdown, autoCommit = false)
  Database.create("MD5", Map())
  private val meta = new H2MetaBackend

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir {
      override def list: Seq[Node] = meta.children(entry.id).map(nodeFor)
      override def mkDir(child: String): Boolean = meta.addNewDir(entry.id, child).isDefined
      override def name: String = entry.name
      override def toString: String = s"Dir '$name': $entry"
    } else new File {
      override def name: String = entry.name
      override def size: Long = 0 // FIXME
      override def toString: String = s"File '$name': $entry"
    }

  override def getNode(path: String): Option[Node] = FileSystem.pathElements(path).flatMap(meta.entry).map(nodeFor)
}

// FIXME remove
object SqlFS extends App {
  val fs = new SqlFS
  val root = fs.getNode("/").get.asInstanceOf[Dir]
  println(root.mkDir("hallo"))
  println(root.mkDir("hallo"))
  println(root.mkDir("welt"))
  val welt = fs.getNode("/welt").get.asInstanceOf[Dir]
  println(welt.mkDir("hello"))
  println(root.list.toList)
}