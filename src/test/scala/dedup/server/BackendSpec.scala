package dedup
package server

import java.util.concurrent.atomic.AtomicBoolean

class BackendSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:
  def repo     = testFile
  val settings = Settings(repo, db.dbDir(repo), java.io.File(repo, "temp"), false, AtomicBoolean(false))
  init("repo" -> repo.getPath)
  val backend  = Backend(settings)
  
  lazy val root = backend.entry("/").get.asInstanceOf[DirEntry]
  "root has ID 0" in assert(root.id == 0)

  lazy val dir1 = backend.mkDir(root.id, "dir1").get
  "create directory" in assert(dir1 == 1)

  "file not found" in
    assert(backend.entry("/dir1/file1") == None)

  lazy val file1 = backend.createAndOpen(dir1, "file1", now).get
  "create a file" in assert(file1 == 2)

  "file is found after create" in
    assert(backend.entry("/dir1/file1").get.isInstanceOf[FileEntry])

  "write some bytes" in
    assert(backend.write(file1, Iterator(4L -> Array[Byte](1, 2, 3, 4), 6L -> Array[Byte](5, 6, 7, 8))))

  "read some bytes past end of file" in
    assert(backend.testRead(file1, 2, 10).toSeq == Seq(0,0,1,2,5,6,7,8))

  "truncate-shorten the file" in
    assert(backend.truncate(file1, 7))

  "read the file past end of file" in
    assert(backend.testRead(file1, 0, 10).toSeq == Seq(0,0,0,0,1,2,5))

  "truncate-extend the file" in
    assert(backend.truncate(file1, 10))

  "release the file" in
    assert(backend.release(file1))

  "release returns false when already closed" in
    assert(!backend.release(file1))

  lazy val file1Entry = Iterator
    .continually { Thread.sleep(10); backend.entry("/dir1/file1").get.asInstanceOf[FileEntry] }
    .filter(_.dataId != DataId(-1)).next
  "get the file entry when persisting is finished as seen by the data ID" in file1Entry

  "re-open the file" in {
    assert(file1Entry.id == 2)
    assert(file1Entry.dataId == DataId(3))
    backend.open(file1Entry)
  }

  "read some bytes at the end, checking the last truncate" in
    assert(backend.testRead(file1, 6, 10).toSeq == Seq(5,0,0,0))

  "release the file again" in
    assert(backend.release(file1))

  "close the backend" in backend.close()
