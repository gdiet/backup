package dedup
package db

class WriteDatabaseSpec extends org.scalatest.freespec.AnyFreeSpec:
  def withDb(f: WriteDatabase => Any): Unit =
    MemH2 { connection => initialize(connection); f(WriteDatabase(connection)) }

  "Tests for mkFile()" - {
    "mkFile good case - note that a file can be created as child of another file" in {
      withDb { db =>
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == Some(1))
        assert(db.mkFile(1, "one", Time(1), DataId(-1)) == Some(2))
      }
    }
    "mkFile fails with exception if the name is empty" in {
      withDb { db =>
        intercept[Exception](db.mkFile(0, "", Time(1), DataId(-1)))
      }
    }
    "mkFile fails normally on name conflict" in {
      withDb { db =>
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == Some(1))
        assert(db.mkFile(0, "one", Time(1), DataId(-1)) == None)
      }
    }
    "mkFile fails with exception if the parent is missing" in {
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

  "Tests for mkDir()" - {
    "mkDir good case" in {
      withDb { db =>
        assert(db.mkDir(0, "one") == Some(1))
        assert(db.mkDir(1, "two") == Some(2))
      }
    }
    "mkDir fails with an exception if the name is empty" in {
      withDb { db =>
        intercept[Exception](db.mkDir(0, ""))
      }
    }
    "mkDir fails normally on name conflict" in {
      withDb { db =>
        assert(db.mkDir(0, "one") == Some(1))
        assert(db.mkDir(0, "one") == None)
      }
    }
    "mkDir fails with exception if the parent is missing" in {
      withDb { db =>
        // ID 0 (root) is the only existing entry.
        // ID 1 is the ID of the entry created next, so it's only sort-of-missing.
        // ID 9 is an 'absolutely missing' ID.
        intercept[Exception](db.mkDir(1, "one"))
        intercept[Exception](db.mkDir(9, "one"))
        // Note: The above attempts have incremented the ID sequence to 3.
        assert(db.mkDir(0, "one") == Some(3))
      }
    }
  }

  "Tests for renameMove()" - {
    "mkFile fails normally on name conflict" in {
      withDb { db =>
        // TODO
      }
    }
  }
