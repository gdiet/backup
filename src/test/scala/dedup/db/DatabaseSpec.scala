package dedup
package db

import scala.collection.SortedMap

class DatabaseSpec extends org.scalatest.freespec.AnyFreeSpec:
  import Database.endOfStorageAndDataGaps

  "Unit tests for the method endOfStorageAndDataGaps(dataChunks: scala.collection.SortedMap[Long, Long])" in {
    assert(endOfStorageAndDataGaps(SortedMap[Long, Long]()) == (0L, Seq()))
    assert(endOfStorageAndDataGaps(SortedMap(10L -> 20L)) == (20L, Seq(DataArea(0, 10))))
    assert(endOfStorageAndDataGaps(SortedMap(0L -> 5L, 10L -> 20L, 30L -> 40L)) == (40L, Seq(DataArea(5, 10), DataArea(20, 30))))
  }
