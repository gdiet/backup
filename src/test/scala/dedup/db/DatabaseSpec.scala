package dedup
package db

import java.sql.{Connection, DriverManager}
import scala.collection.SortedMap
import scala.util.Using.resource

object MemH2:
  Class.forName("org.h2.Driver")
  def apply(f: Connection => Any): Unit = resource(DriverManager.getConnection("jdbc:h2:mem:"))(f)

class DatabaseSpec extends org.scalatest.freespec.AnyFreeSpec:
  import Database.endOfStorageAndDataGaps

  "Unit tests for the method endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long])" in {
    assert(endOfStorageAndDataGaps(SortedMap[Long, Long]()) == (0L, Seq()))
    assert(endOfStorageAndDataGaps(SortedMap(10L -> 20L)) == (20L, Seq(DataArea(0, 10))))
    assert(endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 20L, 30L -> 40L)) == (40L, Seq(DataArea(5, 10), DataArea(20, 30))))
    assert(endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 30L, 30L -> 40L)) == (40L, Seq(DataArea(5, 10))))
    intercept[IllegalArgumentException] { endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 31L, 30L -> 40L)) }
  }

  "Integration test for Database.freeAreas()" in {
    MemH2 { connection =>
      initialize(connection)
      val db = Database(connection)
      // empty database
      assert(db.freeAreas() == Seq(DataArea(0, Long.MaxValue)))
      // one blacklisted entry
      db.insertDataEntry(DataId(1), 1, 10, 0, 0, Array())
      assert(db.freeAreas() == Seq(DataArea(0, Long.MaxValue)))
      // one data entry consisting of two pieces
      db.insertDataEntry(DataId(2), 1, 10,  5, 10, Array())
      db.insertDataEntry(DataId(2), 2, 10, 20, 25, Array())
      assert(db.freeAreas() == Seq(DataArea(0, 5), DataArea(10, 20), DataArea(25, Long.MaxValue)))
      // a duplicate entry start causes a failure
      db.insertDataEntry(DataId(3), 1, 1, 20, 21, Array())
      intercept[IllegalArgumentException] { db.freeAreas() }
    }
  }
