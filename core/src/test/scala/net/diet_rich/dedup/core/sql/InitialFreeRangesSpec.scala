// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import scala.language.reflectiveCalls

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.{Position, DataRange, DataEntryID}

class InitialFreeRangesSpec extends SpecificationWithJUnit { def is = s2"""
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

  def noProblems = dataAreaProblemCheck((10,60), (60,110)) expecting (true)
  def partiallyIdentical = dataAreaProblemCheck((10,50), (10,30)) expecting (false)
  def inclusions = dataAreaProblemCheck((10,50), (20,40)) expecting (false)
  def identical = dataAreaProblemCheck((10,50), (10,50)) expecting (false)
  def partialOverlap = dataAreaProblemCheck((10,30), (20,40)) expecting (false)

  def testSetup[T](dbContents: Seq[(Long, Long)])(f: Session => T): T = InMemoryDB providing { sessionProvider =>
    val tables = new Tables(sessionProvider)
    ranges(dbContents) foreach { tables.createByteStoreEntry(DataEntryID(0), _) }
    f(sessionProvider.session)
  }

  def freeRangesCheck(dbContents: (Long, Long)*) = new {
    def expecting (expectedRanges: (Long, Long)*) = testSetup(dbContents) { session =>
      val actualRanges = DBUtilities.freeRangesInDataArea(session)
      actualRanges should contain(eachOf(ranges(expectedRanges):_*).inOrder)
    }
  }

  def ranges(elems: Seq[(Long, Long)]) = elems map { case (start, fin) => DataRange(Position(start), Position(fin)) }

  def dataAreaProblemCheck(dbContents: (Long, Long)*) = new {
    def expecting (noProblems: Boolean) = testSetup(dbContents) { session =>
      DBUtilities.problemDataAreaOverlaps(session) aka "data overlap problem list" should (
        if (noProblems) beEmpty else not(beEmpty)
      )
    }
  }

}
