package dedup
package db

class WriteDatabaseSpec extends org.scalatest.freespec.AnyFreeSpec:
  def withDb(f: WriteDatabase => Any): Unit =
    MemH2 { connection => initialize(connection); f(WriteDatabase(connection)) }

  "Tests for mkFile()" - {
    "mkFile fails on name conflict" in {
      withDb { db =>
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == Some(1))
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == None)
      }
    }
    "mkFile fails if parent is missing" in {
      withDb { db =>
        // ID 0 (root) is the only existing entry.
        // ID 1 is the ID of the entry created next, so it's only sort-of-missing.
        // ID 9 is an 'absolutely missing' ID.
        intercept[Exception](db.mkFile(1, "one", Time(1), DataId(-1)))
        intercept[Exception](db.mkFile(9, "one", Time(1), DataId(-1)))
        // The above attempts have incremented the ID sequence to 3.
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == Some(3))
      }
    }
  }
