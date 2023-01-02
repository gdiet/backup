package dedup
package db

import java.sql.{Connection, DriverManager}
import scala.collection.SortedMap
import scala.util.Using.resource

class DatabaseSpec extends org.scalatest.freespec.AnyFreeSpec:
  import Database.endOfStorageAndDataGaps

  "Unit tests for the method endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long])" in {
    assert(endOfStorageAndDataGaps(SortedMap[Long, Long]()) == (0L, Seq()))
    assert(endOfStorageAndDataGaps(SortedMap(10L -> 20L)) == (20L, Seq(DataArea(0, 10))))
    assert(endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 20L, 30L -> 40L)) == (40L, Seq(DataArea(5, 10), DataArea(20, 30))))
    assert(endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 30L, 30L -> 40L)) == (40L, Seq(DataArea(5, 10))))
    intercept[IllegalArgumentException] { endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 31L, 30L -> 40L)) }
  }

  "Integration test for Database.freeAreas()" in { MemH2 { connection =>
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
    db.close()
  }}

  "Integration test for Database.pathOf()" in { MemH2 { connection =>
    initialize(connection)
    val db = Database(connection)
    val dir1 = db.mkDir(root.id, "dir1").get
    val dir2 = db.mkDir(dir1, "dir2").get
    val file1 = db.mkFile(dir2, "file1", now, DataId(-1)).get
    val illegalChild = db.mkFile(file1, "illegalChild", now, DataId(-1)).get
    assert(db.pathOf(root.id) == "/")
    assert(db.pathOf(dir1) == "/dir1/")
    assert(db.pathOf(dir2) == "/dir1/dir2/")
    assert(db.pathOf(file1) == "/dir1/dir2/file1")
    intercept[IllegalArgumentException](db.pathOf(illegalChild))
    db.close()
  }}

  "Integration test for Database.mkDir()" in { MemH2 { connection =>
    initialize(connection)
    val db = Database(connection)
    intercept[IllegalArgumentException](db.mkDir(root.id, ""), "Empty name")
    intercept[IllegalArgumentException](db.mkDir(1, "db1"), "Missing parent, special case self-reference")
    intercept[java.sql.SQLException](db.mkDir(99, "db1"), "Missing parent, standard case")
    lazy val db1 = db.mkDir(root.id, "db1").get
    assert(db1 > root.id, "mkdir good case")
    assert(db.mkDir(root.id, "db1") == None, "Name conflict")
    db.close()
  }}
