// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import scala.slick.driver.H2Driver.simple._

import net.diet_rich.dedup.core.values._

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

  private class TestFileSystemData(val sqlTables: SQLTables) extends FileSystemData {
    val dataSettings = new DataSettings { override def blocksize = Size(100) }
  }
  private def withEmptySqlTables[T](f: SQLTables => T) = InMemoryDatabase.withDB { db => f(new SQLTables(db))}
  private def withDataSystem[T](f: FileSystemData => T)(implicit sqlTables: SQLTables) = f(new TestFileSystemData(sqlTables))
  private def range(start: Long, fin: Long = Long.MaxValue) = DataRange(Position(start), Position(fin))
  private def addRangeEntry(start: Long, fin: Long)(implicit sqlTables: SQLTables) =
    sqlTables.createByteStoreEntry(DataEntryID(0), range(start, fin))

  // FIXME implement good matchers
  def partiallyIdentical = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,50)
    addRangeEntry(10,30)
    sqlTables.problemDataAreaOverlaps must not beEmpty
  }

  def identical = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,50)
    addRangeEntry(10,50)
    sqlTables.problemDataAreaOverlaps must not beEmpty
  }

  def inclusions = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,50)
    addRangeEntry(20,40)
    sqlTables.problemDataAreaOverlaps must not beEmpty
  }

  def partialOverlap = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,30)
    addRangeEntry(20,40)
    sqlTables.problemDataAreaOverlaps must not beEmpty
  }

  def gapAndOverlap = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,50)
    addRangeEntry(20,40)
    addRangeEntry(60,110)
    withDataSystem { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(range(110))
      )
    }
  }

  def gap = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry( 0,50)
    addRangeEntry(60,110)
    withDataSystem { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(range(50,60),range(110))
      )
    }
  }

  def entryNotAtStart = withEmptySqlTables { implicit sqlTables =>
    addRangeEntry(10,60)
    addRangeEntry(60,110)
    withDataSystem { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(range(0,10),range(110))
      )
    }
  }

  def emptyDatabase = withEmptySqlTables { implicit sqlTables =>
    withDataSystem { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(List(range(0)))
    }
  }

}
