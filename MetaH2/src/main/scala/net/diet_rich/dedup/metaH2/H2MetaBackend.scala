package net.diet_rich.dedup.metaH2

import java.sql.ResultSet

import net.diet_rich.util.sql._

class H2MetaBackend(implicit connectionFactory: ConnectionFactory) {
  case class TreeEntry(id: Long, key: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long], deleted: Boolean, timestamp: Long)
  object TreeEntry { implicit val result: ResultSet => TreeEntry = { r =>
    TreeEntry(r.long(1), r.long(2), r.long(3), r.string(4), r.longOption(5), r.longOption(6), r.boolean(7), r.long(8))
  } }
  private val prepTreeQueryByParent =
    query[TreeEntry]("SELECT id, key, parent, name, changed, data, deleted, timestamp FROM TreeEntries WHERE parent = ?")

  /** @return The tree entry reachable by the path. */
  def entry(parent: Long): Option[TreeEntry] = {
    val results = prepTreeQueryByParent.run(parent).to[Vector]
    if (results.isEmpty) None else Some(results.maxBy(_.id))
  }
  def entry(path: String): Option[TreeEntry] = {
    ???
  }
}

object Tryout extends App {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl(), H2.user, H2.password, H2.onShutdown)
  Database.create("MD5", Map())
  val meta = new H2MetaBackend
  println(meta.entry(5))
}
