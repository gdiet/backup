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
  """

  private class TestFileSystemData(val sqlTables: SQLTables) extends FileSystemData {
    val dataSettings = new DataSettings { override def blocksize = Size(100) }
  }
  private def withEmptySqlTables[T](f: SQLTables => T) = InMemoryDatabase.withDB { db => f(new SQLTables(db))}
  private def withDataSystem[T](sqlTables: SQLTables)(f: FileSystemData => T) = f(new TestFileSystemData(sqlTables))

  // FIXME implement good matchers
  def inclusions = withEmptySqlTables { sqlTables =>
    sqlTables.createByteStoreEntry(DataEntryID(0), 0, DataRange(Position(10), Position(50)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 1, DataRange(Position(20), Position(30)))
    sqlTables.illegalDataAreaOverlaps must not beEmpty
  }

  def partialOverlap = withEmptySqlTables { sqlTables =>
    sqlTables.createByteStoreEntry(DataEntryID(0), 0, DataRange(Position(10), Position(30)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 1, DataRange(Position(20), Position(50)))
    sqlTables.illegalDataAreaOverlaps must not beEmpty
  }

  def gapAndOverlap = withEmptySqlTables { sqlTables =>
    sqlTables.createByteStoreEntry(DataEntryID(0), 0, DataRange(Position(10), Position(50)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 1, DataRange(Position(20), Position(30)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 2, DataRange(Position(60), Position(110)))
    withDataSystem(sqlTables) { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(
          DataRange(Position(110), Position(Long.MaxValue))
        )
      )
    }
  }

  def gap = withEmptySqlTables { sqlTables =>
    sqlTables.createByteStoreEntry(DataEntryID(0), 0, DataRange(Position(0), Position(50)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 1, DataRange(Position(60), Position(110)))
    withDataSystem(sqlTables) { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(
          DataRange(Position(50), Position(60)),
          DataRange(Position(110), Position(Long.MaxValue))
        )
      )
    }
  }

  def entryNotAtStart = withEmptySqlTables { sqlTables =>
    sqlTables.createByteStoreEntry(DataEntryID(0), 0, DataRange(Position(10), Position(60)))
    sqlTables.createByteStoreEntry(DataEntryID(0), 1, DataRange(Position(60), Position(110)))
    withDataSystem(sqlTables) { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(
        List(
          DataRange(Position(0), Position(10)),
          DataRange(Position(110), Position(Long.MaxValue))
        )
      )
    }
  }

  // FIXME implement good matchers
  def emptyDatabase = withEmptySqlTables { sqlTables =>
    withDataSystem(sqlTables) { dataSystem =>
      dataSystem.freeRangesQueue.toList.reverse should beEqualTo(List(DataRange(Position(0), Position(Long.MaxValue))))
    }
  }

}
