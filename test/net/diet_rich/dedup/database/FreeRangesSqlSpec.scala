// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.util.sql._
import net.diet_rich.util.vals._
import net.diet_rich.dedup.repository.DBConnection
import org.specs2.mutable._

class FreeRangesSqlSpec extends SpecificationWithJUnit {

  implicit val connection = DBConnection.forH2("mem:FreeRangesSqlSpec", false)
  ByteStoreTable.createTable
  val insertEntry = 
    prepareSingleRowUpdate("INSERT INTO ByteStore (dataid, index, start, fin) VALUES (?, ?, ?, ?)")

  sequential
  "The database access methods used in the free ranges management" should {
    "return 0 as start of the free area for a newly created database" in {
      FreeRanges.startOfFreeAreaInDB === Position(0)
    }
    "return no free slices for a newly created database" in {
      FreeRanges.freeSlicesInDB === Set()
    }
    "return the maximum fin as start of the free area" in {
      insertEntry(0, 0, 10, 20)
      FreeRanges.startOfFreeAreaInDB === Position(20)
    }
    "return the free slices in the database" in {
      FreeRanges.freeSlicesInDB === Set(Range(Position(0), Position(10)))
      insertEntry(0, 0, 0, 5)
      FreeRanges.freeSlicesInDB === Set(Range(Position(5), Position(10)))
      insertEntry(0, 0, 25, 30)
      FreeRanges.freeSlicesInDB === Set(Range(Position(5), Position(10)), Range(Position(20), Position(25)))
    }
  }

  "The database used in the test" should {
    "shut down orderly" in {
      execUpdate("shutdown")
      connection.close
    }
  }
}