package net.diet_rich.dedupfs.metadata.sql

import java.io.IOException
import java.sql.{Timestamp, ResultSet}

import net.diet_rich.common._, sql._, vals.Print
import net.diet_rich.dedupfs.StoreMethod
import net.diet_rich.dedupfs.metadata._, TreeEntry._

object Database {
  // Note: All tables are designed for create-only operation, never for update, and delete only when purging to
  // free space. To get the current tree state, a clause like
  //   WHERE id IN (SELECT MAX(id) from TreeEntries GROUP BY key);
  // is needed. Note that this design rules out unique constraints for the tree (i.e., for sibling nodes with
  // the same names).
  private def tableDefinitions(hashAlgorithm: String): Array[String] =
    s"""|CREATE SEQUENCE treeEntryIdSeq START WITH 0;
        |CREATE SEQUENCE treeEntryKeySeq START WITH 0;
        |CREATE TABLE TreeEntries (
        |  id        BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryIdSeq),
        |  key       BIGINT NOT NULL DEFAULT (NEXT VALUE FOR treeEntryKeySeq),
        |  parent    BIGINT NOT NULL,
        |  name      VARCHAR(256) NOT NULL,
        |  changed   BIGINT DEFAULT NULL,
        |  dataid    BIGINT DEFAULT NULL,
        |  deleted   BOOLEAN NOT NULL DEFAULT FALSE,
        |  timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        |  CONSTRAINT pk_TreeEntries PRIMARY KEY (id)
        |);
        |INSERT INTO TreeEntries (parent, name) VALUES (${root.parent}, '${root.name}');
        |CREATE SEQUENCE dataEntryIdSeq START WITH 0;
        |CREATE TABLE DataEntries (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR dataEntryIdSeq),
        |  length BIGINT NOT NULL,
        |  print  BIGINT NOT NULL,
        |  hash   VARBINARY(${Hash digestLength hashAlgorithm}) NOT NULL,
        |  method INTEGER NOT NULL,
        |  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
        |);
        |CREATE SEQUENCE byteStoreIdSeq START WITH 0;
        |CREATE TABLE ByteStore (
        |  id     BIGINT NOT NULL DEFAULT (NEXT VALUE FOR byteStoreIdSeq),
        |  dataid BIGINT NOT NULL,
        |  start  BIGINT NOT NULL,
        |  fin    BIGINT NOT NULL,
        |  CONSTRAINT pk_ByteStore PRIMARY KEY (id)
        |);
        |CREATE TABLE Settings (
        |  key    VARCHAR(256) NOT NULL,
        |  value  VARCHAR(256) NOT NULL,
        |  CONSTRAINT pk_Settings PRIMARY KEY (key)
        |);""".stripMargin split ";"

  private val indexDefinitions: Array[String] =
    """|DROP INDEX idxTreeEntriesParent IF EXISTS;
       |DROP INDEX idxTreeEntriesKey IF EXISTS;
       |CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
       |CREATE INDEX idxTreeEntriesKey ON TreeEntries(key);
       |DROP INDEX idxDataEntriesDuplicates IF EXISTS;
       |CREATE INDEX idxDataEntriesDuplicates ON DataEntries(print, length, hash);
       |DROP INDEX idxByteStoreData IF EXISTS;
       |DROP INDEX idxByteStoreStart IF EXISTS;
       |DROP INDEX idxByteStoreFin IF EXISTS;
       |CREATE INDEX idxByteStoreData ON ByteStore(dataid);
       |CREATE INDEX idxByteStoreStart ON ByteStore(start);
       |CREATE INDEX idxByteStoreFin ON ByteStore(fin);""".stripMargin split ";"

  def create(hashAlgorithm: String, dbSettings: StringMap)(implicit connectionFactory: ConnectionFactory): Unit = {
    tableDefinitions(hashAlgorithm) foreach (update(_).run())
    indexDefinitions foreach (update(_).run())
    // Make sure the empty data entry is stored uncompressed by inserting it manually
    update("INSERT INTO DataEntries (length, print, hash, method) VALUES (?, ?, ?, ?)")
      .run(0, Print.empty, Hash empty hashAlgorithm, StoreMethod.STORE)
    insertSettings(dbSettings)
  }

  private def startOfFreeDataArea(implicit connectionFactory: ConnectionFactory): Long =
    query[Long]("SELECT MAX(fin) FROM ByteStore").run() nextOptionOnly() getOrElse 0

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

  type DataOverlap = (StoreEntry, StoreEntry)
  private[sql] def problemDataAreaOverlaps(implicit connectionFactory: ConnectionFactory): Seq[DataOverlap] = {
    // Note: H2 (1.3.176) does not create a good plan if the three queries are packed into one, and the execution is too slow (two nested table scans)
    val select =
      "SELECT b1.id, b1.dataid, b1.start, b1.fin, b2.id, b2.dataid, b2.start, b2.fin FROM ByteStore b1 JOIN ByteStore b2"
    query[DataOverlap](s"$select ON b1.start < b2.fin AND b1.fin > b2.fin").run().toSeq ++:
    query[DataOverlap](s"$select ON b1.id != b2.id AND b1.start = b2.start").run().toSeq ++:
    query[DataOverlap](s"$select ON b1.id != b2.id AND b1.fin = b2.fin").run().toSeq
  }
  private[sql] def freeRangesInDataArea(implicit connectionFactory: ConnectionFactory): Ranges = {
    dataAreaStarts match {
      case Nil => Nil
      case firstArea :: gapStarts =>
        val tail = dataAreaEnds zip gapStarts
        if (firstArea > 0L) (0L, firstArea) :: tail else tail
    }
  }

  def freeRanges(implicit connectionFactory: ConnectionFactory): Ranges = {
    require(problemDataAreaOverlaps.isEmpty, s"found data area overlaps: $problemDataAreaOverlaps")
    freeRangesInDataArea :+ (startOfFreeDataArea, Long.MaxValue)
  }

  private[sql] def insertSettings(settings: Map[String, String])(implicit connectionFactory: ConnectionFactory): Unit = {
    val prepInsertSettings = singleRowUpdate("INSERT INTO Settings (key, value) VALUES (?, ?)")
    settings foreach prepInsertSettings.run
  }

  private[sql] case class TreeQueryResult(treeEntry: TreeEntry, deleted: Boolean, id: Long, timeOfEntry: Timestamp)

  implicit val longResult: ResultSet => Long = _ long 1
  implicit val boolResult: ResultSet => Boolean = _ boolean 1
  implicit val rangeResult: ResultSet => (Long, Long) = { r => (r long 1, r long 2) }
  implicit val treeEntryResult: ResultSet => TreeEntry = { r => TreeEntry(r long 1, r long 2 , r string 3, r longOption 4, r longOption 5) }
  implicit val treeQueryResult: ResultSet => TreeQueryResult = { r => TreeQueryResult(treeEntryResult(r), r boolean 6, r long 7, r timestamp 8) }
  implicit val storeEntryResult: ResultSet => StoreEntry = { r => StoreEntry(r long 1, r long 2, r long 3, r long 4) }
  implicit val dataEntryResult: ResultSet => DataEntry = { r => DataEntry(r long 1, r long 2, Print(r long 3), r bytes 4, r int 5) }
  implicit val dataAreaResult: ResultSet => (StoreEntry, StoreEntry) = { r => (storeEntryResult(r), StoreEntry(r long 5, r long 6, r long 7, r long 8)) }
  implicit val settingsResult: ResultSet => (String, String) = { r => (r string 1, r string 2) }
}


trait DatabaseRead extends MetadataReadAll { import Database._
  protected implicit def connectionFactory: ConnectionFactory
  protected val repositoryId: String

  // Note: Ideally, here an SQL condition like
  // "deleted IS FALSE AND id IN (SELECT MAX(id) from TreeEntries GROUP BY key)"
  // would be used in the SELECT statement.
  // However, H2 does not run such queries fast enough.
  private val prepTreeChildrenOf = query[TreeQueryResult](
    "SELECT key, parent, name, changed, dataid, deleted, id, timestamp FROM TreeEntries WHERE key IN " +
      "(SELECT key FROM TreeEntries WHERE parent = ?)"
  )
  // FIXME test filterDeleted = None
  override final def treeChildrenOf(parentKey: Long, filterDeleted: Option[Boolean], upToId: Long): Iterable[TreeEntry] =
    prepTreeChildrenOf.runv(parentKey)
      .filter(_.id <= upToId).toSeq            // filter up to id
      .inGroupsOf(_.treeEntry.key)             // group by entry
      .map(_.maxBy(_.id))                      // only the latest version of each entry
      .filter(_.treeEntry.parent == parentKey) // filter by actual parent
      .maybeFilter(filterDeleted, _.deleted == (_:Boolean)) // if applicable, filter by deleted status
      .map(_.treeEntry)                        // get the entries

  override final def allChildren(parent: Long): Iterable[TreeEntry] = treeChildrenOf(parent)
  override final def children(parent: Long): Iterable[TreeEntry] = allChildren(parent).groupBy(_.name).map{ case (_, entries) => entries.head }
  override final def allChildren(parent: Long, name: String): Iterable[TreeEntry] = treeChildrenOf(parent)
  override final def allEntries(path: Array[String]): Iterable[TreeEntry] = path.foldLeft(Iterable(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => allChildren(node.key, name)) }
  override final def entry(path: Array[String]): Option[TreeEntry] = path.foldLeft(Option(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }

  private val prepTreeEntryFor =
    query[TreeQueryResult]("SELECT key, parent, name, changed, dataid, deleted, id, timestamp FROM TreeEntries WHERE key = ?")
  // FIXME test filterDeleted = None
  override final def treeEntryFor(key: Long, filterDeleted: Option[Boolean], upToId: Long): Option[TreeEntry] =
    prepTreeEntryFor.runv(key)
      .filter(_.id <= upToId).toSeq // filter up to id
      .maxOptionBy(_.id)            // only the latest version of the entry
      .maybeFilter(filterDeleted, _.deleted == (_:Boolean)) // if applicable, filter by deleted status
      .map(_.treeEntry)             //  get the entry

  override final def entry(key: Long) = treeEntryFor(key)
  override final def path(key: Long): Option[String] =
    if (key == TreeEntry.root.key) Some(TreeEntry.rootPath)
    else entry(key) flatMap {entry => path(entry.parent) map (_ + "/" + entry.name)}

  // TODO check performance of alternative query "SELECT EXISTS (SELECT 1 FROM DataEntries WHERE print = ?)" (with indexes)
  private val prepDataEntryExistsForPrint = query[Boolean]("SELECT TRUE FROM DataEntries WHERE print = ? LIMIT 1")
  override final def dataEntryExists(print: Print): Boolean = prepDataEntryExistsForPrint runv print nextOption() getOrElse false

  // TODO check performance of alternative query (see above)
  private val prepDataEntryExistsForPrintAndSize = query[Boolean]("SELECT TRUE FROM DataEntries WHERE print = ? AND length = ? LIMIT 1")
  override final def dataEntryExists(size: Long, print: Print): Boolean = prepDataEntryExistsForPrintAndSize runv (print, size) nextOption() getOrElse false

  private val prepSizeOfDataEntry = query[Long]("SELECT length FROM DataEntries WHERE id = ?")
  override final def sizeOf(dataid: Long): Option[Long] = prepSizeOfDataEntry runv dataid nextOption()

  private val prepDataEntriesFor = query[DataEntry]("SELECT * FROM DataEntries WHERE length = ? AND print = ? and hash = ?")
  override final def dataEntriesFor(size: Long, print: Print, hash: Array[Byte]): Seq[DataEntry] = prepDataEntriesFor.runv(size, print, hash).toSeq

  private val prepDataEntryFor = query[DataEntry]("SELECT * FROM DataEntries WHERE id = ?")
  override final def dataEntry(dataid: Long): Option[DataEntry] = prepDataEntryFor runv dataid nextOption()

  private val prepStoreEntriesFor = query[Range]("SELECT start, fin FROM ByteStore WHERE dataid = ? ORDER BY id ASC")
  override final def storeEntries(dataid: Long): Ranges = prepStoreEntriesFor.runv(dataid).toSeq

  private val prepSettings = query[(String, String)]("SELECT key, value FROM Settings")
  override final def settings: Map[String, String] = prepSettings.run().toMap

  override final val hashAlgorithm: String = settings(SQLBackend.hashAlgorithmKey)
  require(settings(SQLBackend.repositoryIdKey) == repositoryId)
}


trait DatabaseWrite extends Metadata { import Database._
  protected implicit val connectionFactory: ConnectionFactory

  private val prepTreeInsert = insertReturnsKey (s"INSERT INTO TreeEntries (parent, name, changed, dataid, deleted) VALUES (?, ?, ?, ?, FALSE)", "key")
  private[sql] final def treeInsert(parent: Long, name: String, changed: Option[Long], data: Option[Long]): Long = prepTreeInsert runv(parent, name, changed, data)
  override final def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = serialized {
    if (parent == TreeEntry.root.parent) throw new IOException("Cannot create a sibling of the root entry")
    treeInsert(parent, name, changed, dataid)
  }
  override final def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = serialized {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override final def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override final def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long = ???

  private val prepTreeUpdate = singleRowUpdate(s"INSERT INTO TreeEntries (key, parent, name, changed, dataid) VALUES (?, ?, ?, ?, ?)")
  final def changeUnchecked(treeEntry: TreeEntry): Unit = serialized { prepTreeUpdate run treeEntry }
  override def change(changed: TreeEntry): Boolean = serialized { init(entry(changed.key).isDefined){ if (_) changeUnchecked(changed) } }

  private val prepTreeDelete = singleRowUpdate(s"INSERT INTO TreeEntries (key, parent, name, changed, dataid, deleted) VALUES (?, ?, ?, ?, ?, TRUE)")
  private[sql] final def treeDelete(treeEntry: TreeEntry): Unit = prepTreeDelete run treeEntry
  override final def delete(entry: TreeEntry): Unit = serialized { treeDelete(entry) }
  override final def delete(key: Long): Boolean = serialized { init(entry(key)){_ foreach delete}.isDefined }

  private val prepNextDataid = query[Long]("SELECT NEXT VALUE FOR dataEntryIdSeq")
  override final def nextDataid() = prepNextDataid run() next()

  private val prepCreateDataEntry = singleRowUpdate(s"INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?)")
  override def createDataEntry(reservedid: Long, size: Long, print: Print, hash: Array[Byte], storeMethod: Int): Unit = prepCreateDataEntry run (reservedid, size, print, hash, storeMethod)

  private val prepCreateByteStoreEntry = singleRowUpdate(s"INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?)")
  override def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit = prepCreateByteStoreEntry run (dataid, start, fin)

  private val prepDeleteSettings = update("DELETE FROM Settings")
  override def replaceSettings(newSettings: Map[String, String]): Unit = serialized {
    prepDeleteSettings run()
    insertSettings(newSettings)
  }

  /** In the API, writing the tree structure is serialized, so e.g. "create only if not exists" can be implemented. */
  override def serialized[T](f: => T): T = synchronized(f)
}
