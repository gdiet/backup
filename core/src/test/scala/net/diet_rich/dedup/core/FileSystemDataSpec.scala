// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import scala.slick.driver.H2Driver.simple._

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.Bytes

class FileSystemDataSpec extends SpecificationWithJUnit with ValueMatchers { def is = s2"""
${"Tests for the file system data area".title}

If the database is empty, the queue should contain one entry only $emptyDatabase
If connected entries not at the start of the area are present, the queue should contain two entries $entryNotAtStart
If entries with gaps are present, the queue should contain appropriate entries $gap
If gaps and illegal overlaps are present, the queue should contain only one entry $gapAndOverlap
Illegal overlaps: partial overlap are correctly detected $partialOverlap
Illegal overlaps: inclusions are correctly detected $inclusions
Illegal overlaps: identical entries are correctly detected $identical
Illegal overlaps: partially identical entries are correctly detected $partiallyIdentical
  """

  private class TestFileSystemData(val sqlTables: SQLTables)
    extends FileSystemData(sqlTables, new DataSettings { override def blocksize = Size(100) }) {
    val freeRangesQueueInTest = freeRangesQueue.reverse
    def writeData(data: Bytes, offset: Position, range: DataRange) = Unit
  }

  def withEmptySqlTables[T](f: SQLTables => T) = InMemoryDatabase.withDB { db => f(new SQLTables(db))}
  def withDataSystem[T](f: TestFileSystemData => T)(implicit sqlTables: SQLTables) = f(new TestFileSystemData(sqlTables))
  def ranges(elems: Seq[(Long, Long)]) = elems map { case (start, fin) => DataRange(Position(start), Position(fin)) }

  def problemDataAreaCheck(elems: (Long, Long)*) = withEmptySqlTables { implicit sqlTables =>
    ranges(elems) foreach { sqlTables.createByteStoreEntry(DataEntryID(0), _) }
    sqlTables.problemDataAreaOverlaps aka "data overlap problem list" must not beEmpty
  }

  def partiallyIdentical = problemDataAreaCheck((10,50), (10,30))
  def identical = problemDataAreaCheck((10,50), (10,50))
  def inclusions = problemDataAreaCheck((10,50), (20,40))
  def partialOverlap = problemDataAreaCheck((10,30), (20,40))

  def freeRangesCheck(input: (Long, Long)*) = new {
    def expecting (expected: (Long, Long)*) = withEmptySqlTables { implicit sqlTables =>
      ranges(input) foreach { sqlTables.createByteStoreEntry(DataEntryID(0), _) }
      withDataSystem { dataSystem =>
        dataSystem.freeRangesQueueInTest should contain(eachOf(ranges(expected):_*).inOrder)
      }
    }
  }

  import language.reflectiveCalls
  def gapAndOverlap = freeRangesCheck ((10,50), (20,40), (60,110)) expecting ((110,Long.MaxValue))
  def gap = freeRangesCheck ((0,50), (60,110)) expecting ((50,60), (110,Long.MaxValue))
  def entryNotAtStart = freeRangesCheck ((10,60), (60,110)) expecting ((0,10),(110,Long.MaxValue))
  def emptyDatabase = freeRangesCheck () expecting ((0,Long.MaxValue))

}
