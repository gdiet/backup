package net.diet_rich.dedupfs.metadata.sql

import java.util.concurrent.atomic.AtomicInteger
import scala.language.reflectiveCalls

import org.specs2.Specification

import net.diet_rich.common.sql
import net.diet_rich.common.sql.{H2, ConnectionFactory}
import net.diet_rich.common.test.TestsHelper
import net.diet_rich.dedupfs.hashAlgorithmKey
import net.diet_rich.dedupfs.metadata._

class InitialFreeRangesSpec extends Specification with TestsHelper { def is = s2"""
Tests for finding the free ranges and problems in the data area:
  If the database is empty, the free ranges list should be empty $emptyDatabase
  If connected entries not at the start of the area are present, the queue should contain the free start area $entriesNotAtStart
  If entries with gaps are present, the queue should contain appropriate entries $gaps
Illegal overlaps:
  No problems are reported for good data $noProblems
  Partially identical entries are correctly detected $partiallyIdentical
  Inclusions are correctly detected $inclusions
  Identical entries are correctly detected $identical
  Partial overlaps are correctly detected $identical
  """

  def emptyDatabase = freeRangesCheck() expecting ()
  def entriesNotAtStart = freeRangesCheck((10,60), (60,110)) expecting ((0,10))
  def gaps = freeRangesCheck((0,50), (60,110), (120, 130)) expecting ((50,60), (110,120))

  def noProblems = dataAreaProblemCheck((10,60), (60,110)) expecting true
  def partiallyIdentical = dataAreaProblemCheck((10,50), (10,30)) expecting false
  def inclusions = dataAreaProblemCheck((10,50), (20,40)) expecting false
  def identical = dataAreaProblemCheck((10,50), (10,50)) expecting false
  def partialOverlap = dataAreaProblemCheck((10,30), (20,40)) expecting false

  def freeRangesCheck(dbContents: Range*) = new {
    def expecting (expectedRanges: Range*) = testSetup(dbContents) { connection =>
      val actualRanges = Database.freeRangesInDataArea(connection)
      actualRanges should beEqualTo(expectedRanges.toList)
    }
  }

  def dataAreaProblemCheck(dbContents: Range*) = new {
    def expecting (noProblems: Boolean) = testSetup(dbContents) { connection =>
      Database.problemDataAreaOverlaps(connection) aka "data overlap problem list" should (
        if (noProblems) beEmpty else not(beEmpty)
        )
    }
  }

  private val dbNum = new AtomicInteger(0)
  def testSetup[T](dbContents: Ranges)(f: ConnectionFactory => T): T = {
    implicit val connectionFactory: ConnectionFactory = H2.memoryFactory(className + s"_${dbNum.incrementAndGet()}")
    Database create ("MD5", Map(hashAlgorithmKey -> "MD5"))
    val prepCreateByteStoreEntry = sql singleRowUpdate s"INSERT INTO ByteStore (dataid, start, fin) VALUES (?, ?, ?)"
    dbContents foreach { case (start, fin) => prepCreateByteStoreEntry run (0, start, fin) }
    f(connectionFactory)
  }
}
