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

/** Methods are not thread safe. Concurrent usage can result in data inconsistencies. */
class H2MetaBackend(implicit connectionFactory: ConnectionFactory) {
  import H2MetaBackend._

  //--------------- tree entries

  case class TreeEntry(id: Long, parent: Long, name: String, changed: Option[Long], data: Option[Long]) {
    def isDir:  Boolean = data.isEmpty
  }
  //noinspection ScalaUnusedSymbol
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

  /** @return the tree entries in reverse order of the path entries. */
  def entries(path: Seq[String]): Nel[Either[String, TreeEntry]] =
    path.foldLeft(Nel(entry(rootId).toRight(rootName))) {
      case (nel, name) => nel.head.fold(_ => Left(name), entry => child(entry.id, name).toRight(name)) :: nel
    }

  private val prepITreeEntry =
    insertReturnsKey("INSERT INTO TreeEntries (parent, name, changed, data) VALUES (?, ?, ?, ?)", "id")
  def mkdir(path: Seq[String]): MkdirResult = {
    entries(path) match {
      case Nel(Left(newName), Right(parent) :: _) =>
        if (parent.isDir) MkdirOk(prepITreeEntry.run(parent.id, newName, None, None))
        else MkdirParentNotADir
      case Nel(Left(_), _) => MkdirParentNotFound
      case Nel(Right(_), _) => MkdirExists
    }
  }

  private val prepIDataEntry =
    insertReturnsKey("INSERT INTO DataEntries () VALUES ()", "id")
  def mkfile(parent: Long, fileName: String): (Long, Long) =
    transaction {
      val dataId = prepIDataEntry.run()
      val id = prepITreeEntry.run(parent, fileName, None, Some(dataId))
      (id, dataId)
    }

  private val prepUTreeEntryMoveRename =
    update("UPDATE TreeEntries SET name = ?, parent = ? WHERE id = ?")
  def moveRename(id: Long, newName: String, newParent: Long): RenameResult = {
    require(id != rootId, s"The root node $rootId is read-only.")
    if (children(newParent).filterNot(_.id == id).exists(_.name == newName)) RenameTargetExists
    else
      prepUTreeEntryMoveRename.run(newName, newParent, id) match {
        case 1 => RenameOk
        case other => assert(other == 1, s"unexpected number of updates in rename: $other"); RenameNotFound
      }
  }

  private val prepDTreeEntry =
    update("DELETE FROM TreeEntries WHERE id = ?")
  def delete(id: Long): DeleteResult =
    if (children(id).nonEmpty) DeleteHasChildren else
      prepDTreeEntry.run(id) match {
        case 1 => DeleteOk
        case other => assert(other == 1, s"unexpected number of updates in delete: $other"); DeleteNotFound
      }

  //--------------- data entries

  case class DataEntry(id: Long, length: Long, hash: Array[Byte]) {
    def isValid: Boolean = length >= 0
  }
  //noinspection ScalaUnusedSymbol
  private object DataEntry { implicit val result: ResultSet => DataEntry = { r =>
    DataEntry(r.long(1), r.long(2), r.bytes(3))
  } }

  //--------------- byte store entries

  implicit val longOptionResult: ResultSet => Option[Long] = _.longOption(1)
  implicit val longResult: ResultSet => Long = _.long(1)

  private def startOfFreeDataArea(implicit connectionFactory: ConnectionFactory): Long =
    query[Option[Long]]("SELECT MAX(fin) FROM ByteStore").run().nextOnly().getOrElse(0)

  private def dataAreaStarts(implicit connectionFactory: ConnectionFactory): List[Long] =
    query[Long](
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 " +
        "ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
    ).run().toList

  private def dataAreaEnds(implicit connectionFactory: ConnectionFactory): List[Long] =
    query[Long](
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 " +
        "ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
    ).run().toList

  private def freeRangesInDataArea(implicit connectionFactory: ConnectionFactory): List[(Long, Long)] = {
    dataAreaStarts match {
      case Nil => Nil
      case firstArea :: gapStarts =>
        val tail = dataAreaEnds zip gapStarts
        if (firstArea > 0L) (0L, firstArea) :: tail else tail
    }
  }

  case class StoreEntry (id: Long, dataId: Long, start: Long, fin: Long)
  object StoreEntry { implicit val result: ResultSet => StoreEntry = { r =>
    StoreEntry(r.long(1), r.long(2), r.long(3), r.long(4))
  } }

  private type DataOverlap = (StoreEntry, StoreEntry)
  implicit val dataOverlapResult: ResultSet => DataOverlap = r =>
    (StoreEntry.result(r), StoreEntry(r.long(5), r.long(6), r.long(7), r.long(8)))
  private def problemDataAreaOverlaps(implicit connectionFactory: ConnectionFactory) = {
    // Note: H2 (1.3.176) does not create a good plan if the three queries are packed into one, and the execution is too slow (two nested table scans)
    val select =
      "SELECT b1.id, b1.dataId, b1.start, b1.fin, b2.id, b2.dataId, b2.start, b2.fin FROM ByteStore b1 JOIN ByteStore b2"
    query[DataOverlap](s"$select ON b1.start < b2.fin AND b1.fin > b2.fin").run().toSeq ++:
      query[DataOverlap](s"$select ON b1.id != b2.id AND b1.start = b2.start").run().toSeq ++:
      query[DataOverlap](s"$select ON b1.id != b2.id AND b1.fin = b2.fin").run().toSeq
  }

  def freeRanges(implicit connectionFactory: ConnectionFactory): List[(Long, Long)] = {
    require(problemDataAreaOverlaps.isEmpty, s"found data area overlaps: $problemDataAreaOverlaps")
    freeRangesInDataArea :+ (startOfFreeDataArea, Long.MaxValue)
  }
}
