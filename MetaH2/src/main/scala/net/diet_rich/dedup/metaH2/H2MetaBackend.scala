package net.diet_rich.dedup.metaH2

import java.sql.ResultSet

import net.diet_rich.util._
import net.diet_rich.util.fs._
import net.diet_rich.util.sql._

object H2MetaBackend {
  val rootsParent = 0
  val rootId: Int = rootsParent + 1
  val rootName = ""
}

/** Methods are not thread safe, meaning that concurrent usage might result in data inconsistencies. */
class H2MetaBackend(implicit connectionFactory: ConnectionFactory) {
  import H2MetaBackend._

  case class TreeEntry(id: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long]) {
    def isDir:  Boolean = data.isEmpty
    def isFile: Boolean = data.isDefined
  }
  //noinspection ScalaUnusedSymbol
  private object TreeEntry { implicit val result: ResultSet => TreeEntry = { r =>
    TreeEntry(r.long(1), r.long(2), r.string(3), r.longOption(4), r.longOption(5))
  } }

  private val prepQJournal = query[JournalEntry]("SELECT * FROM TreeJournal")
  case class JournalEntry(id: Long, treeId: Long, parent: Option[Long], name: Option[String], changed: Option[Long], data: Option[Long], deleted: Boolean, timestamp: Option[Long])
  //noinspection ScalaUnusedSymbol
  private object JournalEntry { implicit val result: ResultSet => JournalEntry = { r =>
    JournalEntry(r.long(1), r.long(2), r.longOption(3), r.stringOption(4), r.longOption(5), r.longOption(6), r.boolean(7), r.longOption(8))
  }}

  private val selectTreeEntry = "SELECT id, parent, name, changed, data FROM TreeEntries"

  private val prepQTreeById = query[TreeEntry](s"$selectTreeEntry WHERE id = ?")
  def entry(id: Long): Option[TreeEntry] = prepQTreeById.run(id).nextOptionOnly()

  private val prepQTreeByParent = query[TreeEntry](s"$selectTreeEntry WHERE parent = ?")
  def children(parent: Long): Seq[TreeEntry] = prepQTreeByParent.run(parent).toSeq

  private val prepQTreeByParentName = query[TreeEntry](s"$selectTreeEntry WHERE parent = ? AND name = ?")
  def child(parent: Long, name: String): Option[TreeEntry] = prepQTreeByParentName.run(parent, name).nextOptionOnly()

  /** @return the tree entries in reverse order of the path entries. */
  def entries(path: Seq[String]): Nel[Either[String, TreeEntry]] =
    path.foldLeft(Nel(entry(rootId).toRight(rootName))) {
      case (nel, name) => nel.head.fold(_ => Left(name), entry => child(entry.id, name).toRight(name)) :: nel
    }

  private val prepITreeEntry =
    insertReturnsKey("INSERT INTO TreeEntries (parent, name, changed, data) VALUES (?, ?, ?, ?)", "id")
  private val prepITreeJournal =
    insert("INSERT INTO TreeJournal (treeId, parent, name, changed, data, deleted, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)")
  def mkdir(path: List[String]): MkdirResult = {
    entries(path) match {
      case Nel(Left(newName), Right(parent) :: _) =>
        if (!parent.isDir) MkdirParentNotADir else
          MkdirOk(init(prepITreeEntry.run(parent.id, newName, None, None))(
            prepITreeJournal.run(_, parent.id, newName, None, None, false, None)
          ))
      case Nel(Left(_), _) => MkdirParentNotFound
      case Nel(Right(_), _) => MkdirExists
    }
  }

  // FIXME double-check where to use insert, update, singleRowUpdate, ...
  private val prepUTreeEntryRename =
    singleRowUpdate("UPDATE TreeEntries SET name = ?, parent = ? WHERE id = ?")
  def rename(id: Long, newName: String, newParent: Long): RenameResult =
    if (children(newParent).filterNot(_.id == id).exists(_.name == newName)) RenameTargetExists
    else transaction {
      // FIXME try-catch for "not found"
      prepUTreeEntryRename.run(newName, newParent, id)
      prepITreeJournal.run(id, newParent, newName, None, None, false, None)
      RenameOk
    }

  private val prepDTreeEntry =
    singleRowUpdate("DELETE FROM TreeEntries WHERE id = ?")
  def delete(id: Long): DeleteResult =
    try if (children(id).nonEmpty) DeleteHasChildren else transaction {
      prepDTreeEntry.run(id)
      prepITreeJournal.run(id, None, None, None, None, true, None)
      DeleteOk
    } catch { case _: IllegalStateException => DeleteNotFound }
}
