package net.diet_rich.fusefs

import net.diet_rich.dedup.metaH2.{Database, H2, H2MetaBackend}
import net.diet_rich.scalafs.{Dir, File, FileSystem, Node}
import net.diet_rich.util.sql.ConnectionFactory

class SqlFS extends FileSystem {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl(), H2.user, H2.password, H2.onShutdown, autoCommit = false)
  Database.create("MD5", Map())
  private val meta = new H2MetaBackend
  meta.addNewDir(H2MetaBackend.rootId, "hallo")
  private val welt = meta.addNewDir(H2MetaBackend.rootId, "welt").get
  meta.addNewDir(welt, "hello")
  meta.addNewDir(welt, "world")

  private def nodeFor(entry: meta.TreeEntry): Node =
    if (entry.isDir) new Dir {
      override def name: String = entry.name
      override def list: Seq[Node] = meta.children(entry.id).map(nodeFor)
    } else new File {
      override def name: String = entry.name
      override def size: Long = 0 // FIXME
    }

  override def getNode(path: String): Option[Node] = meta.entry(path).map(nodeFor)
}
