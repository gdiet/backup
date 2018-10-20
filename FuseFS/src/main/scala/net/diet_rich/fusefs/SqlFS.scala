package net.diet_rich.fusefs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.scalafs._
import net.diet_rich.util.sql.ConnectionFactory

class SqlFS extends FileSystem {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl, H2.defaultUser, H2.defaultPassword, H2.memoryOnShutdown)
  Database.create("MD5", Map())
  private val meta = new H2MetaBackend
  // FIXME remove
  val root = getNode("/").get.asInstanceOf[Dir]
  println(root.mkDir("hallo"))
  println(root.mkDir("hallo"))
  println(root.mkDir("welt"))
  val welt = getNode("/welt").get.asInstanceOf[Dir]
  println(welt.mkDir("hello"))
  println(root.list.toList)

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir {
      override def list: Seq[Node] = meta.children(entry.id).map(nodeFor)
      override def mkDir(child: String): Boolean = meta.addNewDir(entry.id, child).isDefined
      override def name: String = entry.name
      override def renameTo(path: String): RenameResult = ??? // FIXME rename is ready, move isn't
      override def toString: String = s"Dir '$name': $entry"
    } else new File {
      override def name: String = entry.name
      override def size: Long = 0 // FIXME
      override def renameTo(path: String): RenameResult = ???
      override def toString: String = s"File '$name': $entry"
    }

  override def getNode(path: String): Option[Node] = FileSystem.pathElements(path).flatMap(meta.entry).map(nodeFor)
}
