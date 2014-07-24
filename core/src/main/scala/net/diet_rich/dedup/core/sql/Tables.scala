package net.diet_rich.dedup.core.sql

import scala.slick.jdbc.{SetParameter, GetResult, StaticQuery}

import net.diet_rich.dedup.core.{Lifecycle, FileSystem}
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.{Equal, init}

trait TablesPart extends SessionSlice with Lifecycle {
  object tables {
    import TableQueries._

    // TreeEntries
    def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id).firstOption
    def treeChildren(parent: TreeEntryID): List[TreeEntry] = treeChildrenForParentQuery(parent).list
    def createTreeEntry(parent: TreeEntryID, name: String, changed: Option[Time], dataid: Option[DataEntryID]): TreeEntry = inTransaction {
      val id = nextTreeEntryIdQuery.first
      createTreeEntryUpdate(id, parent, name, changed, dataid).execute
      TreeEntry(id, parent, name, changed, dataid, None)
    }
    def markDeleted(id: TreeEntryID, deletionTime: Option[Time]): Boolean = setTreeEntryDeletedUpdate(deletionTime, id).first === 1
    def moveRename(id: TreeEntryID, newParent: TreeEntryID, newName: String): Boolean = setTreeEntryPathUpdate(newName, newParent, id).first === 1

    // DataEntries
    def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id).firstOption
    def dataEntries(size: Size, print: Print): List[DataEntry] = dataEntriesForSizePrintQuery(size, print).list
    def dataEntries(size: Size, print: Print, hash: Hash): List[DataEntry] = dataEntriesForSizePrintHashQuery(size, print, hash).list
    def createDataEntry(reservedID: DataEntryID, size: Size, print: Print, hash: Hash, method: StoreMethod): Unit = inTransaction(
      createDataEntryUpdate(reservedID, size, print, hash, method).execute
    )
    def nextDataID: DataEntryID = nextDataEntryIdQuery.first

    // ByteStore
    def storeEntries(id: DataEntryID): List[StoreEntry] = storeEntriesForIdQuery(id).list
    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit = inTransaction(
      createStoreEntryUpdate(dataid, range.start, range.fin).execute
    )

    // Note: Writing is synchronized, so "create only if not exists" can be implemented.
    def inTransaction[T](f: => T): T = synchronized(f)
  }

  abstract override def setup() = {
    super.setup()
    require(tables.treeEntry(FileSystem.ROOTID) === Some(FileSystem.ROOTENTRY))
  }
}

object TableQueries {
  // atomic results
  implicit val _getDataEntryId       = GetResult(r => DataEntryID(r nextLong()))
  implicit val _getDataEntryIdOption = GetResult(r => DataEntryID(r nextLongOption()))
  implicit val _getHash              = GetResult(r => Hash(r nextBytes()))
  implicit val _getPosition          = GetResult(r => Position(r nextLong()))
  implicit val _getPrint             = GetResult(r => Print(r nextLong()))
  implicit val _getSize              = GetResult(r => Size(r nextLong()))
  implicit val _getStoreEntryId      = GetResult(r => StoreEntryID(r nextLong()))
  implicit val _getStoreMethod       = GetResult(r => StoreMethod(r nextInt()))
  implicit val _getTimeOption        = GetResult(r => Time(r nextLongOption()))
  implicit val _getTreeEntryId       = GetResult(r => TreeEntryID(r nextLong()))

  // compound results - order of definition is important
  implicit val _getDataEntry         = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
  implicit val _getDataRange         = GetResult(r => new DataRange(r <<, r <<))
  implicit val _getStoreEntry        = GetResult(r => StoreEntry(r <<, r <<, r <<))
  implicit val _getTreeEntry         = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))

  // parameter Setters
  implicit val _setHash              = SetParameter((v: Hash, p) => p setBytes v.value)
  implicit val _setIntValue          = SetParameter((v: IntValue, p) => p setInt v.value)
  implicit val _setLongValue         = SetParameter((v: LongValue, p) => p setLong v.value)
  implicit val _setLongValueOption   = SetParameter((v: Option[LongValue], p) => p setLongOption (v map (_ value)))

  // basic select statements
  private val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
  private val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
  private val selectFromByteStore = "SELECT id, dataid, start, fin FROM ByteStore"

  // TreeEntries
  val treeEntryForIdQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
  val treeChildrenForParentQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
  val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")
  val setTreeEntryDeletedUpdate = StaticQuery.update[(Option[Time], TreeEntryID)]("UPDATE TreeEntries SET deleted = ? WHERE id = ?;")
  val setTreeEntryPathUpdate = StaticQuery.update[(String, TreeEntryID, TreeEntryID)]("UPDATE TreeEntries SET name = ?, parent = ? WHERE id = ?;")
  val createTreeEntryUpdate = StaticQuery.update[(TreeEntryID, TreeEntryID, String, Option[Time], Option[DataEntryID])]("INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES (?, ?, ?, ?, ?);")

  // DataEntries
  val dataEntryForIdQuery = StaticQuery.query[DataEntryID, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
  val dataEntriesForSizePrintQuery = StaticQuery.query[(Size, Print), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
  val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Size, Print, Hash), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
  val nextDataEntryIdQuery = StaticQuery.queryNA[DataEntryID]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")
  val createDataEntryUpdate = StaticQuery.update[(DataEntryID, Size, Print, Hash, StoreMethod)]("INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?);")

  // ByteStore
  val storeEntriesForIdQuery = StaticQuery.query[DataEntryID, StoreEntry](s"$selectFromByteStore WHERE dataid = ? ORDER BY id ASC;")
  val createStoreEntryUpdate = StaticQuery.update[(DataEntryID, Position, Position)]("INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?);")
}
