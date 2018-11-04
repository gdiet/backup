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
  def mkdir(path: Seq[String]): MkdirResult = {
    entries(path) match {
      case Nel(Left(newName), Right(parent) :: _) =>
        if (!parent.isDir) MkdirParentNotADir else
          MkdirOk(transaction(init(prepITreeEntry.run(parent.id, newName, None, None))(
            prepITreeJournal.run(_, parent.id, newName, None, None, false, None)
          )))
      case Nel(Left(_), _) => MkdirParentNotFound
      case Nel(Right(_), _) => MkdirExists
    }
  }

  private val prepIDataEntry =
    insertReturnsKey("INSERT INTO DataEntries () VALUES ()", "id")
  def mkfile(parent: Long, fileName: String): (Long, Long) =
    transaction {
      val dataId = prepIDataEntry.run()
      val id = init(prepITreeEntry.run(parent, fileName, None, Some(dataId)))(
        prepITreeJournal.run(_, parent, fileName, None, Some(dataId), false, None)
      )
      (id, dataId)
    }

  private val prepUTreeEntryMoveRename =
    update("UPDATE TreeEntries SET name = ?, parent = ? WHERE id = ?")
  def moveRename(id: Long, newName: String, newParent: Long): RenameResult =
    if (children(newParent).filterNot(_.id == id).exists(_.name == newName)) RenameTargetExists
    else transaction {
      prepUTreeEntryMoveRename.run(newName, newParent, id) match {
        case 1 =>
          prepITreeJournal.run(id, newParent, newName, None, None, false, None)
          RenameOk
        case other =>
          assert(other == 1, s"unexpected number of updates in rename: $other")
          RenameNotFound
      }
    }

  private val prepDTreeEntry =
    update("DELETE FROM TreeEntries WHERE id = ?")
  def delete(id: Long): DeleteResult =
    if (children(id).nonEmpty) DeleteHasChildren else transaction {
      prepDTreeEntry.run(id) match {
        case 1 =>
          prepITreeJournal.run(id, None, None, None, None, true, None)
          DeleteOk
        case other =>
          assert(other == 1, s"unexpected number of updates in delete: $other")
          DeleteNotFound
      }
    }
}
