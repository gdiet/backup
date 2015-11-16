package net.diet_rich.dedupfs.metadata.sql

import java.io.IOException
import java.sql.{Connection, ResultSet}

import net.diet_rich.common._, net.diet_rich.common.sql._
import net.diet_rich.common.sql
import net.diet_rich.dedupfs.StoreMethod
import net.diet_rich.dedupfs.metadata._, TreeEntry._

object Database {
  // Note: All tables are designed for create-only operation,
  // never for update, and delete only when purging to free space.
  // To get the current tree state, a clause like
  //   WHERE id IN (SELECT MAX(id) from TreeEntries GROUP BY key);
  // is needed.
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

  private val indexDefinitions: Array[String] = // TODO review index definitions with regard to TreeEntry id and to combined indexes
    """|DROP INDEX idxTreeEntriesParent IF EXISTS;
       |DROP INDEX idxTreeEntriesDataid IF EXISTS;
       |DROP INDEX idxTreeEntriesDeleted IF EXISTS;
       |CREATE INDEX idxTreeEntriesParent ON TreeEntries(parent);
       |CREATE INDEX idxTreeEntriesDataid ON TreeEntries(dataid);
       |CREATE INDEX idxTreeEntriesDeleted ON TreeEntries(deleted);
       |DROP INDEX idxDataEntriesDuplicates IF EXISTS;
       |DROP INDEX idxDataEntriesLengthPrint IF EXISTS;
       |CREATE INDEX idxDataEntriesDuplicates ON DataEntries(length, print, hash);
       |CREATE INDEX idxDataEntriesLengthPrint ON DataEntries(length, print);
       |DROP INDEX idxByteStoreData IF EXISTS;
       |DROP INDEX idxByteStoreStart IF EXISTS;
       |DROP INDEX idxByteStoreFin IF EXISTS;
       |CREATE INDEX idxByteStoreData ON ByteStore(dataid);
       |CREATE INDEX idxByteStoreStart ON ByteStore(start);
       |CREATE INDEX idxByteStoreFin ON ByteStore(fin);""".stripMargin split ";"

  def create(hashAlgorithm: String)(implicit connection: Connection): Unit = {
    tableDefinitions(hashAlgorithm) foreach (sql.update(_).run())
    indexDefinitions foreach (sql.update(_).run())
    // Make sure the empty data entry is stored plain by inserting it manually
    sql.update("INSERT INTO DataEntries (length, print, hash, method) VALUES (?, ?, ?, ?)")
      .run(0, printOf(Bytes.empty), Hash empty hashAlgorithm, StoreMethod.STORE)
  }

  private def startOfFreeDataArea(implicit connection: Connection): Long =
    sql.query[Long]("SELECT MAX(fin) FROM ByteStore").run() nextOptionOnly() getOrElse 0

  private def dataAreaStarts(implicit connection: Connection): List[Long] =
    sql.query[Long](
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 " +
      "ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start"
    ).run().toList
  private def dataAreaEnds(implicit connection: Connection): List[Long] =
    sql.query[Long](
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 " +
      "ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin"
    ).run().toList

  type DataOverlap = (StoreEntry, StoreEntry)
  private def problemDataAreaOverlaps(implicit connection: Connection): Seq[DataOverlap] = {
    // Note: H2 (1.3.176) does not create a good plan if the three queries are packed into one, and the execution is too slow (two nested table scans)
    val select =
      "SELECT b1.id, b1.dataid, b1.start, b1.fin, b2.id, b2.dataid, b2.start, b2.fin FROM ByteStore b1 JOIN ByteStore b2"
    sql.query[DataOverlap](s"$select ON b1.start < b2.fin AND b1.fin > b2.fin").run().toSeq ++:
    sql.query[DataOverlap](s"$select ON b1.id != b2.id AND b1.start = b2.start").run().toSeq ++:
    sql.query[DataOverlap](s"$select ON b1.id != b2.id AND b1.fin = b2.fin").run().toSeq
  }
  private def freeRangesInDataArea(implicit connection: Connection): Ranges = {
    dataAreaStarts match {
      case Nil => Nil
      case firstArea :: gapStarts =>
        val tail = dataAreaEnds zip gapStarts
        if (firstArea > 0L) (0L, firstArea) :: tail else tail
    }
  }

  def freeRanges(implicit connection: Connection): Ranges = {
    require(problemDataAreaOverlaps.isEmpty, s"found data area overlaps: $problemDataAreaOverlaps")
    freeRangesInDataArea :+ (startOfFreeDataArea, Long.MaxValue)
  }

  implicit val longResult = {(_: ResultSet) long 1}
  implicit val boolResult = {(_: ResultSet) boolean 1}
  implicit val rangeResult = { r: ResultSet => (r long 1, r long 2) }
  implicit val treeEntryResult = { r: ResultSet => TreeEntry(r long 1, r long 2 , r string 3, r longOption 4, r longOption 5) }
  implicit val treeQueryResult = { r: ResultSet => (treeEntryResult(r), r boolean 6, r long 7) }
  implicit val storeEntryResult = { r: ResultSet => StoreEntry(r long 1, r long 2, r long 3, r long 4) }
  implicit val dataEntryResult = { r: ResultSet => DataEntry(r long 1, r long 2, r long 3, r bytes 4, r int 5) }
  implicit val dataAreaResult = { r: ResultSet => (storeEntryResult(r), StoreEntry(r long 5, r long 6, r long 7, r long 8)) }
}


trait DatabaseRead extends MetadataRead { import Database._
  private[sql] implicit def connection: Connection

  // Note: Ideally, here an SQL condition like
  // "deleted IS FALSE AND id IN (SELECT MAX(id) from TreeEntries GROUP BY key)"
  // would be used in the SELECT statement.
  // However, H2 does not run such queries fast enough.
  private val prepTreeChildrenOf =
    sql.query[(TreeEntry, Boolean, Long)](s"SELECT key, parent, name, changed, dataid, deleted, id, timestamp FROM TreeEntries WHERE parent = ?")
  override final def treeChildrenOf(parentKey: Long, isDeleted: Boolean = false, upToId: Long = Long.MaxValue): Iterable[TreeEntry] =
    prepTreeChildrenOf.runv(parentKey)
      .filter { case (_, _, id) => id <= upToId }
      .toSeq
      .groupBy { case (treeEntry, _, _) => treeEntry.key }
      .map { case (key, entries) => entries.maxBy { case (_, _, id) => id } }
      .filter { case (_, deleted, _) => deleted == isDeleted }
      .map { case (treeEntry, _, _) => treeEntry }
  override final def allChildren(parent: Long): Iterable[TreeEntry] = treeChildrenOf(parent)
  override final def children(parent: Long): Iterable[TreeEntry] = allChildren(parent).groupBy(_.name).map{ case (_, entries) => entries.head }
  override final def allChildren(parent: Long, name: String): Iterable[TreeEntry] = treeChildrenOf(parent)
  override final def child(parent: Long, name: String): Option[TreeEntry] = treeChildrenOf(parent) find (_.name == name)
  override final def allEntries(path: Array[String]): Iterable[TreeEntry] = path.foldLeft(Iterable(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => allChildren(node.key, name)) }
  override final def entry(path: Array[String]): Option[TreeEntry] = path.foldLeft(Option(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }

  private val prepTreeEntryFor =
    sql.query[(TreeEntry, Boolean, Long)]("SELECT key, parent, name, changed, dataid, deleted, id, timestamp FROM TreeEntries WHERE key = ?")
  override final def treeEntryFor(key: Long, isDeleted: Boolean = false, upToId: Long = Long.MaxValue): Option[TreeEntry] =
    prepTreeEntryFor.runv(key)
      .find { case (_, deleted, id) => deleted == isDeleted && id <= upToId }
      .map { case (treeEntry, _, _) => treeEntry }
  override final def entry(key: Long) = treeEntryFor(key)
  override final def path(key: Long): Option[String] =
    if (key == TreeEntry.root.key) Some(TreeEntry.rootPath)
    else entry(key) flatMap {entry => path(entry.parent) map (_ + "/" + entry.name)}

  // TODO check performance of alternative query "SELECT EXISTS (SELECT 1 FROM DataEntries WHERE print = ?)"
  private val prepDataEntryExistsForPrint = sql.query[Boolean]("SELECT TRUE FROM DataEntries WHERE print = ? LIMIT 1")
  override final def dataEntryExists(print: Long): Boolean = prepDataEntryExistsForPrint runv print nextOption() getOrElse false

  // TODO check performance of alternative query (see above)
  private val prepDataEntryExistsForPrintAndSize = sql.query[Boolean]("SELECT TRUE FROM DataEntries WHERE print = ? AND length = ? LIMIT 1")
  override final def dataEntryExists(print: Long, size: Long): Boolean = prepDataEntryExistsForPrintAndSize runv (print, size) nextOption() getOrElse false

  private val prepSizeOfDataEntry = sql.query[Long]("SELECT length FROM DataEntries WHERE id = ?")
  override final def sizeOf(dataid: Long): Option[Long] = prepSizeOfDataEntry runv dataid nextOption()

  private val prepDataEntriesFor = sql.query[DataEntry]("SELECT * FROM DataEntries WHERE length = ? AND print = ? and hash = ?")
  override final def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): Seq[DataEntry] = prepDataEntriesFor.runv(size, print, hash).toSeq

  private val prepDataEntryFor = sql.query[DataEntry]("SELECT * FROM DataEntries WHERE id = ?")
  override final def dataEntry(dataid: Long): Option[DataEntry] = prepDataEntryFor runv dataid nextOption()

  private val prepStoreEntriesFor = sql.query[Range]("SELECT start, fin FROM ByteStore WHERE dataid = ? ORDER BY id ASC")
  override final def storeEntries(dataid: Long): Ranges = prepStoreEntriesFor.runv(dataid).toSeq

  override final def settings: Map[String, String] = ???
}


trait DatabaseWrite extends Metadata { import Database._
  private[sql] implicit val connectionFactory: ScalaThreadLocal[Connection]
  private[sql] implicit def connection: Connection

  private val prepTreeInsert = sql insertReturnsKey (s"INSERT INTO TreeEntries (parent, name, changed, dataid, deleted) VALUES (?, ?, ?, ?, FALSE)", "key")
  private[sql] final def treeInsert(parent: Long, name: String, changed: Option[Long], data: Option[Long]): Long = prepTreeInsert runv(parent, name, changed, data)
  override final def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = inTransaction {
    if (parent == TreeEntry.root.parent) throw new IOException("Cannot create a sibling of the root entry")
    treeInsert(parent, name, changed, dataid)
  }
  override final def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = inTransaction {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override final def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override final def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long = ???

  private val prepTreeUpdate = sql singleRowUpdate s"INSERT INTO TreeEntries (key, parent, name, changed, dataid) VALUES (?, ?, ?, ?, ?)"
  final def changeUnchecked(treeEntry: TreeEntry): Unit = inTransaction { prepTreeUpdate run treeEntry }
  override def change(changed: TreeEntry): Boolean = inTransaction { init(entry(changed.key).isDefined){ if (_) changeUnchecked(changed) } }

  private val prepTreeDelete = sql singleRowUpdate s"INSERT INTO TreeEntries (key, parent, name, changed, dataid, deleted) VALUES (?, ?, ?, ?, ?, TRUE)"
  private[sql] final def treeDelete(treeEntry: TreeEntry): Unit = prepTreeDelete run treeEntry
  override final def delete(entry: TreeEntry): Unit = inTransaction { treeDelete(entry) }
  override final def delete(key: Long): Boolean = inTransaction { init(entry(key)){_ foreach delete}.isDefined }

  private val prepNextDataid = sql.query[Long]("SELECT NEXT VALUE FOR dataEntryIdSeq")
  override final def nextDataid() = prepNextDataid run() next()

  private val prepCreateDataEntry = sql singleRowUpdate s"INSERT INTO DataEntries (id, length, print, hash, method) VALUES (?, ?, ?, ?, ?)"
  override def createDataEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit = prepCreateDataEntry run (reservedid, size, print, hash, storeMethod)

  private val prepCreateByteStoreEntry = sql singleRowUpdate s"INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?)"
  override def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit = prepCreateByteStoreEntry run (dataid, start, fin)

  override def replaceSettings(newSettings: Map[String, String]): Unit = ???

  /** In the API, (only) writing the tree structure is synchronized, so "create only if not exists" can be implemented. */
  override def inTransaction[T](f: => T): T = synchronized(f)
}

/* TODO use or delete this old code

  object Testutil {
    private val dbid = new AtomicLong(0L)

    def memoryDB: SessionFactory = SessionFactory(
      Database forURL(
        // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
        url = s"jdbc:h2:mem:testdb_${dbid incrementAndGet}",
        user = "sa", driver = "org.h2.Driver"
      ),
      readWrite
    )
  }


  class InitialFreeRangesSpec extends Specification { def is = s2"""
  ${"Tests for finding the free ranges and problems in the data area".title}

  If the database is empty, the free ranges list should be empty $emptyDatabase
  If connected entries not at the start of the area are present, the queue should contain the free start area $entriesNotAtStart
  If entries with gaps are present, the queue should contain appropriate entries $gaps

  Illegal overlaps: No problems are reported for good data $noProblems
  Illegal overlaps: Partially identical entries are correctly detected $partiallyIdentical
  Illegal overlaps: Inclusions are correctly detected $inclusions
  Illegal overlaps: Identical entries are correctly detected $identical
  Illegal overlaps: Partial overlaps are correctly detected $identical
    """

    def emptyDatabase = freeRangesCheck () expecting ()
    def entriesNotAtStart = freeRangesCheck ((10,60), (60,110)) expecting ((0,10))
    def gaps = freeRangesCheck ((0,50), (60,110), (120, 130)) expecting ((50,60), (110,120))

    def noProblems = dataAreaProblemCheck((10,60), (60,110)) expecting true
    def partiallyIdentical = dataAreaProblemCheck((10,50), (10,30)) expecting false
    def inclusions = dataAreaProblemCheck((10,50), (20,40)) expecting false
    def identical = dataAreaProblemCheck((10,50), (10,50)) expecting false
    def partialOverlap = dataAreaProblemCheck((10,30), (20,40)) expecting false

    def freeRangesCheck(dbContents: StartFin*) = new {
      def expecting (expectedRanges: StartFin*) = testSetup(dbContents) { session =>
        val actualRanges = DBUtilities.freeRangesInDataArea(session)
        actualRanges should beEqualTo(expectedRanges.toList)
      }
    }

    def dataAreaProblemCheck(dbContents: StartFin*) = new {
      def expecting (noProblems: Boolean) = testSetup(dbContents) { session =>
        DBUtilities.problemDataAreaOverlaps(session) aka "data overlap problem list" should (
          if (noProblems) beEmpty else not(beEmpty)
          )
      }
    }

    def testSetup[T](dbContents: Seq[StartFin])(f: CurrentSession => T): T = {
      val db = Testutil.memoryDB
      DBUtilities.createTables("MD5")(db session)
      val meta = new SQLMetaBackend(db)
      dbContents foreach { case (start, fin) => meta.createByteStoreEntry(0, start, fin) }
      f(db session)
    }
  }

*/
