package dedup
package db

import java.io.*
import java.nio.charset.StandardCharsets
import scala.util.Using.resource

class BlacklistSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:

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
        "black1d" -> black1d.asLong,
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

  "processInternalBlacklist doesn't delete copies in the blacklist directory" in {
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

  "externalFilesToInternalBlacklist works..." in {

    // create physical test files
    assert(testFile.mkdirs(), s"testFile.mkdirs returned false - $testFile")
    val tdir = File(testFile, "dir1/dir2")
    assert(tdir.mkdirs(), s"tdir.mkdirs returned false - $tdir")
    resource(FileWriter(File(tdir, "file1"), StandardCharsets.UTF_8))(_.write("file content"))
    resource(FileWriter(File(tdir, "file2"), StandardCharsets.UTF_8))(_.write("other content"))
    resource(FileWriter(File(tdir, "file3"), StandardCharsets.UTF_8))(_.write("file content"))

    dbWithContents { case (db, ids) =>
      // blacklist
      blacklist.externalFilesToInternalBlacklist(db, testFile, ids("blacklist"), false)
      intercept[IllegalArgumentException](
        // Time stamp in generated outside, so this should fail due to name conflict.
        blacklist.externalFilesToInternalBlacklist(db, testFile, ids("blacklist"), false)
      )

      // check virtual files
      val file1 = db.entry("/blacklist/dir1/dir2/file1").get.asInstanceOf[FileEntry]
      val file2 = db.entry("/blacklist/dir1/dir2/file2").get.asInstanceOf[FileEntry]
      val file3 = db.entry("/blacklist/dir1/dir2/file3").get.asInstanceOf[FileEntry]
      assert(file1.dataId == file3.dataId)
      assert(file2.dataId != file3.dataId)
      assert(db.dataSize(file1.dataId) == 12)
      assert(db.dataSize(file2.dataId) == 13)
      assert(db.storageSize(file1.dataId) == 0)
      assert(db.storageSize(file2.dataId) == 0)

      // check physical files, first with delete=false, then delete=true
      assert(File(testFile, "dir1/dir2/file1").isFile)
      val black2 = db.mkDir(root.id, "black2").get
      blacklist.externalFilesToInternalBlacklist(db, testFile, black2, true)
      assert(!File(testFile, "dir1/dir2/file1").exists())

      // some more checks on the second blacklist copy
      val file4 = db.entry("/black2/dir1/dir2/file1").get.asInstanceOf[FileEntry]
      assert(file1.dataId == file4.dataId)
      assert(db.dataSize(file4.dataId) == 12)
      assert(db.storageSize(file4.dataId) == 0)
    }
  }
