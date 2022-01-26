package dedup
package db

class BlacklistSpec extends org.scalatest.freespec.AnyFreeSpec:

  def dbWithContents(f: (Database, Map[String, Long]) => Any): Unit =
    MemH2 { connection =>
      initialize(connection)
      val db = Database(connection)
      val blacklist = db.mkDir(root.id, "blacklist").get
      val black1 = db.mkFile(blacklist, "black1", now, DataId(-1)).get
      val black1d = db.newDataIdFor(black1)
      db.insertDataEntry(black1d, 1, 10, 10, 20, Array())
      val black2 = db.mkFile(blacklist, "black2", now, black1d).get
      val dir1 = db.mkDir(root.id, "dir1").get
      val file1 = db.mkFile(dir1, "file1", now, black1d).get
      f(db, Map(
        "blacklist" -> blacklist,
        "black1" -> black1,
        "black1d" -> black1d.toLong,
        "black2" -> black2,
        "file1" -> file1
      ))
    }

  "processInternalBlacklist removes the storage allocation of blacklist files" in {
    dbWithContents { case (db, ids) =>
      assert(db.parts(DataId(ids("black1d"))) == Vector((10, 10)))
      blacklist.processInternalBlacklist(db, "blacklist", "/blacklist", ids("blacklist"), false)
      assert(db.parts(DataId(ids("black1d"))) == Vector())
    }
  }

  "processInternalBlacklist doesn't delete copies in the blacklist folder" in {
    dbWithContents { case (db, ids) =>
      blacklist.processInternalBlacklist(db, "blacklist", "/blacklist", ids("blacklist"), true)
      assert(db.entry(ids("black1")).get.getClass == classOf[FileEntry])
      assert(db.entry(ids("black2")).get.getClass == classOf[FileEntry])
    }
  }

  "processInternalBlacklist doesn't delete copies when requested not to" in {
    dbWithContents { case (db, ids) =>
      blacklist.processInternalBlacklist(db, "blacklist", "/blacklist", ids("blacklist"), false)
      assert(db.entry(ids("black1")).get.getClass == classOf[FileEntry])
      assert(db.entry(ids("black2")).get.getClass == classOf[FileEntry])
      assert(db.entry(ids("file1")).get.getClass == classOf[FileEntry])
    }
  }

  "processInternalBlacklist deletes copies when requested to" in {
    dbWithContents { case (db, ids) =>
      blacklist.processInternalBlacklist(db, "blacklist", "/blacklist", ids("blacklist"), true)
      assert(db.entry(ids("black1")).get.getClass == classOf[FileEntry])
      assert(db.entry(ids("black2")).get.getClass == classOf[FileEntry])
      assert(db.entry(ids("file1")) == None)
    }
  }
