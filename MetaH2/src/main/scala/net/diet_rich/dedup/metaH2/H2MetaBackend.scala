package net.diet_rich.dedup.metaH2

import java.sql.ResultSet

import net.diet_rich.util.sql._

object H2MetaBackend {
  val pathSeparator = "/"
  val rootId = 0
  val rootName = ""
  val rootPath = "/"

  def pathElements(path: String): Option[Array[String]] = {
    if (!path.startsWith(pathSeparator)) None else
    if (path == rootPath) Some(Array()) else
    Some(path.split(pathSeparator).drop(1))
  }
}

class H2MetaBackend(implicit connectionFactory: ConnectionFactory) {
  import H2MetaBackend._

  case class TreeEntry(id: Long, key: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long], deleted: Boolean, timestamp: Long) {
    def isDir: Boolean = data.isEmpty
    def isFile: Boolean = data.isDefined
  }
  object TreeEntry { implicit val result: ResultSet => TreeEntry = { r =>
    TreeEntry(r.long(1), r.long(2), r.long(3), r.string(4), r.longOption(5), r.longOption(6), r.boolean(7), r.long(8))
  } }

  private val prepTreeQueryByKey =
    query[TreeEntry]("SELECT id, key, parent, name, changed, data, deleted, timestamp FROM TreeEntries WHERE key = ?")
  def entry(key: Long): Option[TreeEntry] = {
    val results = prepTreeQueryByParent.run(key).toSeq
    if (results.isEmpty) None else Some(results.maxBy(_.id))
  }

  private val prepTreeQueryByParent =
    query[TreeEntry]("SELECT id, key, parent, name, changed, data, deleted, timestamp FROM TreeEntries WHERE parent = ?")
  def children(parent: Long): Seq[TreeEntry] = {
    prepTreeQueryByParent
      .run(parent).toSeq
      .groupBy(_.name)
      .values
      .map(_.filterNot(_.deleted))
      .filter(_.nonEmpty)
      .map(_.maxBy(_.id))
      .toSeq
  }

  private val prepTreeQueryByParentAndName =
    query[TreeEntry]("SELECT id, key, parent, name, changed, data, deleted, timestamp FROM TreeEntries WHERE parent = ? AND name = ?")
  def child(parent: Long, name: String): Option[TreeEntry] = {
    val results = prepTreeQueryByParentAndName.run(parent, name).toSeq
    if (results.isEmpty) None else Some(results.maxBy(_.id))
  }

  def entry(path: String): Option[TreeEntry] =
    pathElements(path).flatMap(entry(rootId, _))
  def entry(key: Long, children: Array[String]): Option[TreeEntry] =
    if (children.isEmpty) entry(key) else
    child(key, children(0)).flatMap(e => entry(e.key, children.drop(1)))

  private val prepInsertEntry =
    insertReturnsKey("INSERT INTO TreeEntries (parent, name, changed, data, timestamp) VALUES (?, ?, ?, ?, ?)", "key")
  def addNewDir(parent: Long, name: String): Option[Long] = {
    entry(parent).flatMap { parentEntry =>
      if (!parentEntry.isDir) None else {
        ???
      }
    }
  }
}

object Tryout extends App {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl(), H2.user, H2.password, H2.onShutdown)
  Database.create("MD5", Map())
  val meta = new H2MetaBackend
  println(meta.children(5))
}
