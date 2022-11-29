package dedup
package server

import java.util.concurrent.atomic.AtomicBoolean

// FIXME now obsolete, kept only for reference
class Level1Spec extends org.scalatest.freespec.AnyFreeSpec with TestFile:
  def repo = testFile
  val settings = Settings(repo, db.dbDir(repo), java.io.File(repo, "temp"), false, AtomicBoolean(false))
  init("repo" -> repo.getPath)
  val level1 = Level1(settings)

  lazy val root = level1.entry("/").get.asInstanceOf[DirEntry]
  "root has ID 0" in assert(root.id == 0)

  lazy val dirId = level1.mkDir(root.id, "dir").get
  "create directory" in assert(dirId == 1)

  lazy val tenBytesId = level1.createAndOpen(dirId, "10 bytes", now).get
  "create a 10 bytes file" in {
    assert(tenBytesId == 2)
    assert(level1.write(tenBytesId, Iterator(4L -> Array[Byte](1, 2, 3, 4), 6L -> Array[Byte](5, 6, 7, 8))))
  }

  "read the open 10 bytes file" in {
    val sink = new Array[Byte](10)
    assert(level1.read(tenBytesId, 2, 10, sink) == Some(8))
    assert(sink.toSeq == Seq(0,0,1,2,5,6,7,8,0,0))
  }

  "truncate-shorten the open 10 bytes file" in {
    val sink = new Array[Byte](7)
    assert(level1.truncate(tenBytesId, 7))
    assert(level1.read(tenBytesId, 0, 7, sink) == Some(7))
    assert(sink.toSeq == Seq(0,0,0,0,1,2,5))
  }

  "truncate-extends the open 10 bytes file" in {
    val sink = new Array[Byte](5)
    assert(level1.truncate(tenBytesId, 10))
    assert(level1.read(tenBytesId, 4, 4, sink) == Some(4))
    assert(sink.toSeq == Seq(1,2,5,0,0))
  }

  "release(1) the open 10 bytes file" in assert(level1.release(tenBytesId))

  "release returns false when already closed" in assert(!level1.release(tenBytesId))

  lazy val tenBytes =
    Iterator
      // Wait for persist to finish, seen by the data ID becoming != -1.
      .continually { Thread.sleep(10); level1.entry("/dir/10 bytes").get.asInstanceOf[FileEntry] }
      .filter(_.dataId != DataId(-1)).next
  "re-open the open 10 bytes file after some time" in {
    assert(tenBytes.id == 2)
    assert(tenBytes.dataId == DataId(3))
    level1.open(tenBytes)
  }

  "read(2) the open 10 bytes file" in {
    val sink = new Array[Byte](10)
    assert(level1.read(tenBytesId, 0, 10, sink) == Some(10))
    assert(sink.toSeq == Seq(0,0,0,0,1,2,5,0,0,0))
  }

  "release(2) the open 10 bytes file" in assert(level1.release(tenBytesId))

  "close level 1" in level1.close()
