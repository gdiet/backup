package net.diet_rich.dedup.core.sql

import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.StaticQuery.interpolation

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.{Equal, init}

import DBUtilities._

// FIXME define slice
trait TablesPart extends SessionSlice {
  object tables {
    // basic select statements
    private val selectFromTreeEntries = "SELECT id, parent, name, changed, dataid, deleted FROM TreeEntries"
    private val selectFromDataEntries = "SELECT id, length, print, hash, method FROM DataEntries"
    private val selectFromByteStore = "SELECT id, dataid, start, fin FROM ByteStore"

    // TreeEntries
    private val treeEntryForIdQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE id = ?;")
    private val treeChildrenForParentQuery = StaticQuery.query[TreeEntryID, TreeEntry](s"$selectFromTreeEntries WHERE parent = ?;")
    private val nextTreeEntryIdQuery = StaticQuery.queryNA[TreeEntryID]("SELECT NEXT VALUE FOR treeEntriesIdSeq;")

    def treeEntry(id: TreeEntryID): Option[TreeEntry] = treeEntryForIdQuery(id) firstOption

    def treeChildren(parent: TreeEntryID): List[TreeEntry] = treeChildrenForParentQuery(parent) list

    def createTreeEntry(parent: TreeEntryID, name: String, changed: Option[Time], dataid: Option[DataEntryID]): TreeEntryID = inTransaction {
      init(nextTreeEntryIdQuery first) {
        id => sqlu"INSERT INTO TreeEntries (id, parent, name, changed, dataid) VALUES ($id, $parent, $name, $changed, $dataid);" execute
      }
    }

    // DataEntries
    private val dataEntryForIdQuery = StaticQuery.query[DataEntryID, DataEntry](s"$selectFromDataEntries WHERE id = ?;")
    private val dataEntriesForSizePrintQuery = StaticQuery.query[(Size, Print), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ?;")
    private val dataEntriesForSizePrintHashQuery = StaticQuery.query[(Size, Print, Hash), DataEntry](s"$selectFromDataEntries WHERE length = ? AND print = ? AND hash = ?;")
    private val nextDataEntryIdQuery = StaticQuery.queryNA[DataEntryID]("SELECT NEXT VALUE FOR dataEntriesIdSeq;")

    def dataEntry(id: DataEntryID): Option[DataEntry] = dataEntryForIdQuery(id) firstOption

    def dataEntries(size: Size, print: Print): List[DataEntry] = dataEntriesForSizePrintQuery(size, print) list

    def dataEntries(size: Size, print: Print, hash: Hash): List[DataEntry] = dataEntriesForSizePrintHashQuery(size, print, hash) list

    def createDataEntry(reservedID: DataEntryID, size: Size, print: Print, hash: Hash, method: StoreMethod): Unit = inTransaction(
      sqlu"INSERT INTO DataEntries (id, length, print, hash, method) VALUES ($reservedID, $size, $print, $hash, $method);" execute
    )

    def nextDataID: DataEntryID = nextDataEntryIdQuery first

    // ByteStore
    private val storeEntriesForIdQuery = StaticQuery.query[DataEntryID, StoreEntry](s"$selectFromByteStore WHERE dataid = ? ORDER BY id ASC;")

    def storeEntries(id: DataEntryID): List[StoreEntry] = storeEntriesForIdQuery(id) list

    def createByteStoreEntry(dataid: DataEntryID, range: DataRange): Unit = inTransaction(
      sqlu"INSERT INTO ByteStore (dataid, start, fin) VALUES ($dataid, ${range.start}, ${range.fin});" execute // TODO can we use range directly here?
    )

    // general
    def inTransaction[T](f: => T): T = synchronized(f)

    // startup checks
    def runChecks = { // FIXME lifecycle
      require(treeEntry(FileSystem.ROOTID) === Some(FileSystem.ROOTENTRY))
    }
  }
}
