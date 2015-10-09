package net.diet_rich.dedupfs.metadata

import java.sql.Connection

import net.diet_rich.common._
import net.diet_rich.dedupfs.StoreMethod

import sql.WrappedSQLResult
import TreeEntry.root

object Database {
  // Note: All tables are designed for create-only operation,
  // never for update, and delete only when purging to free space.
  // To get the current tree state, a clause like
  //   WHERE id IN (SELECT MAX(id) from TreeEntries GROUP BY key);
  // is needed.
  def tableDefinitions(hashAlgorithm: String): Array[String] =
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
        |INSERT INTO TreeEntries (parent, name, timestamp) VALUES (${root.parent}, '${root.name}', 0);
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

  val indexDefinitions: Array[String] = // TODO review index definitions with regard to TreeEntry id and to combined indexes
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
    tableDefinitions(hashAlgorithm) foreach (sql.update(_))
    indexDefinitions foreach (sql.update(_))
    // Make sure the empty data entry is stored plain by inserting it manually
    sql.update("INSERT INTO DataEntries (length, print, hash, method) VALUES (?, ?, ?, ?);")
      .run(0, printOf(Bytes.empty), Hash empty hashAlgorithm, StoreMethod.STORE)
  }

  val currentNotDeletedTreeEntry = "deleted IS FALSE AND id IN (SELECT MAX(id) from TreeEntries GROUP BY key)"


  def startOfFreeDataArea(implicit connection: Connection): Long =
    sql.query("SELECT MAX(fin) FROM ByteStore;").runv()(_ long 1) nextOptionOnly() getOrElse 0
}

class TreeDatabase(treeCondition: String)(implicit connectionFactory: ScalaThreadLocal[Connection]) {
  assert(treeCondition.nonEmpty)
  private implicit def connection: Connection = connectionFactory()
  import scala.language.implicitConversions
  private implicit def productToSeq(product: Product): Seq[Any] = product.productIterator.toSeq

  private val prepTreeChildrenOf =
    sql query s"SELECT (id, parent, name, changed, data) FROM TreeEntries WHERE parent = ? AND $treeCondition"
  def treeChildrenOf(parentKey: Long): Seq[TreeEntry] =
    prepTreeChildrenOf.runv(parentKey)(r => TreeEntry(r.long(1), r.long(2), r.string(3), r.longOption(4), r.longOption(5))).toSeq

  private val prepTreeUpdate =
    sql update s"INSERT INTO TreeEntries(id, parent, name, changed, data) VALUES (?, ?, ?, ?, ?)"
  def treeUpdate(treeEntry: TreeEntry): Unit =
    prepTreeUpdate.run(treeEntry)

  private val prepTreeDelete =
    sql update s"INSERT INTO TreeEntries(id, parent, name, changed, data, deleted) VALUES (?, ?, ?, ?, ?, TRUE)"
  def treeDelete(treeEntry: TreeEntry): Unit =
    prepTreeDelete.run(treeEntry)
}

  /*
    private def startOfFreeDataArea(implicit session: CurrentSession) = StaticQuery.queryNA[Long](
      "SELECT MAX(fin) FROM ByteStore;"
    ).firstOption getOrElse 0L
    private def dataAreaEnds(implicit session: CurrentSession): List[Long] = StaticQuery.queryNA[Long](
      "SELECT b1.fin FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.fin = b2.start WHERE b2.start IS NULL ORDER BY b1.fin;"
    ).list
    private def dataAreaStarts(implicit session: CurrentSession): List[Long] = StaticQuery.queryNA[Long](
      "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL ORDER BY b1.start;"
    ).list
    def problemDataAreaOverlaps(implicit session: CurrentSession): List[(StoreEntry, StoreEntry)] = {
      // Note: H2 (1.3.176) does not create a good plan if the three queries are packed into one, and the execution is too slow (two nested table scans)
      val select = "SELECT b1.id, b1.dataid, b1.start, b1.fin, b2.id, b2.dataid, b2.start, b2.fin FROM ByteStore b1 JOIN ByteStore b2"
      StaticQuery.queryNA[(StoreEntry, StoreEntry)](s"$select ON b1.start < b2.fin AND b1.fin > b2.fin;").list :::
      StaticQuery.queryNA[(StoreEntry, StoreEntry)](s"$select ON b1.id != b2.id AND b1.start = b2.start;").list :::
      StaticQuery.queryNA[(StoreEntry, StoreEntry)](s"$select ON b1.id != b2.id AND b1.fin = b2.fin;").list
    }
    def freeRangesInDataArea(implicit session: CurrentSession): List[StartFin] = {
      dataAreaStarts match {
        case Nil => Nil
        case firstArea :: gapStarts =>
          val tail = dataAreaEnds zip gapStarts
          if (firstArea > 0L) (0L, firstArea) :: tail else tail
      }
    }
    def freeRangeAtEndOfDataArea(implicit session: CurrentSession): StartFin = (startOfFreeDataArea, Long.MaxValue)
    def freeAndProblemRanges(implicit session: CurrentSession): (Ranges, List[(StoreEntry, StoreEntry)]) = {
      val problemRanges = DBUtilities.problemDataAreaOverlaps
      val freeInData = if (problemRanges isEmpty) DBUtilities.freeRangesInDataArea else Nil
      val freeRanges = freeInData.toVector :+ DBUtilities.freeRangeAtEndOfDataArea
      (freeRanges, problemRanges)
    }


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
