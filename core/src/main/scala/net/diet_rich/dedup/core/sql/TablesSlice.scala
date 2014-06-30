package net.diet_rich.dedup.core.sql

import scala.slick.jdbc.GetResult
import scala.slick.jdbc.SetParameter
import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.StaticQuery.interpolation

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.init

trait TablesSlice extends DatabasePart {
  protected final object tables {
    // atomic results
    private implicit val _getDataEntryId       = GetResult(r => DataEntryID(r nextLong))
    private implicit val _getDataEntryIdOption = GetResult(r => DataEntryID(r nextLongOption))
    private implicit val _getHash              = GetResult(r => Hash(r nextBytes))
    private implicit val _getPosition          = GetResult(r => Position(r nextLong))
    private implicit val _getPrint             = GetResult(r => Print(r nextLong))
    private implicit val _getSize              = GetResult(r => Size(r nextLong))
    private implicit val _getStoreEntryId      = GetResult(r => StoreEntryID(r nextLong))
    private implicit val _getStoreMethod       = GetResult(r => StoreMethod(r nextInt))
    private implicit val _getTimeOption        = GetResult(r => Time(r nextLongOption))
    private implicit val _getTreeEntryId       = GetResult(r => TreeEntryID(r nextLong))

    // compound results - order of definition is important
    private implicit val _getDataEntry         = GetResult(r => DataEntry(r <<, r <<, r <<, r <<, r <<))
    private implicit val _getDataRange         = GetResult(r => DataRange(r <<, r <<))
    private implicit val _getStoreEntry        = GetResult(r => StoreEntry(r <<, r <<, r <<))
    private implicit val _getTreeEntry         = GetResult(r => TreeEntry(r <<, r <<, r <<, r <<, r <<, r <<))

    // parameter setters
    private implicit val _setHash            = SetParameter((v: Hash, p) => p setBytes v.value)
    private implicit val _setIntValue        = SetParameter((v: IntValue, p) => p setInt v.value)
    private implicit val _setLongValue       = SetParameter((v: LongValue, p) => p setLong v.value)
    private implicit val _setLongValueOption = SetParameter((v: Option[LongValue], p) => p setLongOption (v map (_ value)))

    // basic select statements
    private val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
    private val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
    private val selectFromByteStore = "SELECT id, dataid, start, fin FROM ByteStore"
    private val selectFromSettings = "SELECT key, value FROM Settings"

    // TreeEntries
    private val treeEntryForIdQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
    private val treeChildrenForParentQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
    private val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")

    // DataEntries
    private val dataEntryForIdQuery = StaticQuery.query[DataEntryID, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
    private val dataEntriesForSizePrintQuery = StaticQuery.query[(Size, Print), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
    private val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Size, Print, Hash), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
    private val nextDataEntryIdQuery = StaticQuery.queryNA[DataEntryID]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")

    // ByteStore
    private val storeEntriesForIdQuery = StaticQuery.query[DataEntryID, StoreEntry](s"$selectFromByteStore WHERE dataid = ? ORDER BY id ASC;")

    // TreeEntries
    def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id) firstOption
    def treeChildren(parent: TreeEntryID): List[TreeEntry] = treeChildrenForParentQuery(parent) list
    def createTreeEntry(parent: TreeEntryID, name: String, changed: Option[Time], dataid: Option[DataEntryID]): TreeEntryID = inTransaction {
      init(nextTreeEntryIdQuery first) {
        id => sqlu"INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES ($id, $parent, $name, $changed, $dataid);" execute
      }
    }

    // DataEntries
    def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id) firstOption
    def dataEntries(size: Size, print: Print): List[DataEntry] = dataEntriesForSizePrintQuery(size, print) list
    def dataEntries(size: Size, print: Print, hash: Hash): List[DataEntry] = dataEntriesForSizePrintHashQuery(size, print, hash) list
    def createDataEntry(size: Size, print: Print, hash: Hash, method: StoreMethod): DataEntryID = inTransaction (
      init(nextDataID) {
        id => sqlu"INSERT INTO DataEntries (id, length, print, hash, method) VALUES ($id, $size, $print, $hash, $method);" execute
      }
    )
    def nextDataID: DataEntryID = nextDataEntryIdQuery first

    // ByteStore
    def startOfFreeDataArea = StaticQuery.queryNA[Position]("SELECT MAX(fin) FROM ByteStore;").firstOption getOrElse Position(0)
    def dataAreaEnds: List[Position] = StaticQuery.queryNA[Position](
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin;"
    ).list
    def dataAreaStarts: List[Position] = StaticQuery.queryNA[Position](
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start;"
    ).list
    def problemDataAreaOverlaps: List[(StoreEntry, StoreEntry)] = StaticQuery.queryNA[(StoreEntry, StoreEntry)](
      """|SELECT b1.id, b1.dataid, b1.start, b1.fin, b2.id, b2.dataid, b2.start, b2.fin
        |  FROM ByteStore b1 JOIN ByteStore b2 ON
        |    (b1.id != b2.id AND (b1.start = b2.start OR b1.fin = b2.fin)) OR
        |    (b1.start < b2.fin AND b1.fin > b2.fin);""".stripMargin
    ).list

    def storeEntries(id: DataEntryID): List[StoreEntry] = storeEntriesForIdQuery(id) list
    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit = inTransaction (
      sqlu"INSERT INTO ByteStore (dataid, start, fin) VALUES ($dataid, ${range.start}, ${range.fin});" execute // TODO can we use range directly here?
    )

    // general
    def inTransaction[T] (f: => T): T = synchronized(f)

    // startup checks
    require(treeEntry(FileSystem.ROOTID) == Some(FileSystem.ROOTENTRY))
  }
}
