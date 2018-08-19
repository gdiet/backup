package net.diet_rich.dedup.metaH2

import java.sql.ResultSet

import net.diet_rich.util.init
import net.diet_rich.util.sql._

object H2MetaBackend {
  val pathSeparator = "/"
  val rootsParent = 0
  val rootId: Int = rootsParent + 1
  val rootName = ""
  val rootPath = "/"

  private def pathElements(path: String): Option[Seq[String]] = {
    if (!path.startsWith(pathSeparator)) None else
    if (path == rootPath) Some(Seq()) else
    Some(path.split(pathSeparator).toList.drop(1))
  }

  def splitParentPath(path: String): Option[(String, String)] =
    if (!path.startsWith(pathSeparator) || path == rootPath) None else {
      Some(path.lastIndexOf('/') match {
        case 0 => ("/", path.drop(1))
        case i => (path.take(i), path.drop(i+1))
      })
    }
}

class H2MetaBackend(implicit connectionFactory: ConnectionFactory) {
  import H2MetaBackend._

  case class TreeEntry(id: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long]) {
    def isDir:  Boolean = data.isEmpty
    def isFile: Boolean = data.isDefined
  }
  private object TreeEntry { implicit val result: ResultSet => TreeEntry = { r =>
    TreeEntry(r.long(1), r.long(2), r.string(3), r.longOption(4), r.longOption(5))
  } }

  private val selectTreeEntry = "SELECT id, parent, name, changed, data FROM TreeEntries"

  private val prepQTreeById = query[TreeEntry](s"$selectTreeEntry WHERE id = ?")
  def entry(id: Long): Option[TreeEntry] = prepQTreeById.run(id).nextOptionOnly()

  private val prepQTreeByParent = query[TreeEntry](s"$selectTreeEntry WHERE parent = ?")
  def children(parent: Long): Seq[TreeEntry] = prepQTreeByParent.run(parent).toSeq

  private val prepQTreeByParentName = query[TreeEntry](s"$selectTreeEntry WHERE parent = ? AND name = ?")
  def child(parent: Long, name: String): Option[TreeEntry] = prepQTreeByParentName.run(parent, name).nextOptionOnly()

  def entry(path: String): Option[TreeEntry] = pathElements(path).flatMap(entry(rootId, _))
  private def entry(id: Long, children: Seq[String]): Option[TreeEntry] =
    if (children.isEmpty) entry(id) else child(id, children.head).flatMap(e => entry(e.id, children.tail))

  private val prepITreeEntry =
    insertReturnsKey("INSERT INTO TreeEntries (parent, name, changed, data) VALUES (?, ?, ?, ?)", "id")
  private val prepITreeJournal =
    insert("INSERT INTO TreeJournal (treeId, parent, name, changed, data, timestamp) VALUES (?, ?, ?, ?, ?, ?)")
  def addNewDir(parent: Long, name: String): Option[Long] = entry(parent).flatMap { parentEntry =>
    if (!parentEntry.isDir) None else {
      child(parent, name) match {
        case Some(_) => None
        case None => connectionFactory.transaction {
          Some(init(prepITreeEntry.run(parent, name, None, None))(
            prepITreeJournal.run(_, parent, name, None, None, System.currentTimeMillis())
          ))
        }
      }
    }
  }
}

object Tryout extends App {
  private implicit val connectionFactory: ConnectionFactory =
    ConnectionFactory(H2.jdbcMemoryUrl(), H2.user, H2.password, H2.onShutdown, autoCommit = false)
  Database.create("MD5", Map())
  val meta = new H2MetaBackend
  println(meta.addNewDir(1, "a"))
  println(meta.addNewDir(1, ""))
  println(meta.addNewDir(2, ""))
}
