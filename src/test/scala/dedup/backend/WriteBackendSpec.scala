package dedup
package backend

import java.util.concurrent.atomic.AtomicBoolean

class WriteBackendSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile:
  def bytes(data: Int*) = data.map(_.toByte).toArray
  extension(result: Option[Iterator[(Long, Array[Byte])]])
    def data: Seq[(Long, Seq[Byte])] = result.get.toSeq.map(e => e._1 -> e._2.toSeq)

  def repo = testFile

  val settings = server.Settings(repo, db.dbDir(repo), java.io.File(repo, "temp"), false, AtomicBoolean(false))
  init("repo" -> repo.getPath)
  val fs: WriteBackend = dedup.backend(settings).asInstanceOf[WriteBackend]

  lazy val root = fs.entry("/").get.asInstanceOf[DirEntry]
  "root has ID 0" in assert(root.id == 0)

  lazy val dirId = fs.mkDir(root.id, "dir").get
  "create directory" in assert(dirId == 1)

  lazy val tenBytesId = fs.createAndOpen(dirId, "10 bytes", now).get
  "create a 10 bytes file" in {
    assert(tenBytesId == 2)
    assert(fs.write(tenBytesId, Iterator(4L -> bytes(1, 2, 3, 4), 6L -> bytes(5, 6, 7, 8))))
  }

  "read the open 10 bytes file" in {
    // Also trying to read past end-of-file...
    assert(fs.read(tenBytesId, 2, 10).data == Seq(2L -> Seq(0, 0), 4L -> Seq(1, 2, 5, 6, 7, 8)))
  }

  "truncate-shorten the open 10 bytes file" in {
    assert(fs.truncate(tenBytesId, 7))
    assert(fs.read(tenBytesId, 0, 7).data == Seq(0L -> Seq(0,0,0,0), 4L -> Seq(1,2,5)))
  }

  "truncate-extends the open 10 bytes file" in {
    assert(fs.truncate(tenBytesId, 10))
    assert(fs.read(tenBytesId, 4, 4).data == Seq(4L -> Seq(1,2,5), 7L -> Seq(0)))
  }

  "release the open 10 bytes file" in {
    // When released here, the actual dataId is not yet known, so it's -1.
    assert(fs.release(tenBytesId) == Some(-1))
  }

  "release returns None when already closed" in {
    assert(fs.release(tenBytesId) == None)
  }

  val dataId = DataId(3)
  "wait for persist to finish, signaled by the file getting a data ID" in {
    val file = Iterator.continually {
      Thread.sleep(2)
      fs.entry("/dir/10 bytes").get.asInstanceOf[FileEntry]
        .tap(println)
    }.find(_.dataId != DataId(-1)).get
    assert(file.dataId == dataId)
  }

  "reopen the 10 bytes file" in {
    fs.open(tenBytesId, dataId)
  }

  "read the reopened 10 bytes file" in {
    // Also trying to read past end-of-file...
    assert(fs.read(tenBytesId, 0, 12).data == Seq(0L -> Seq(0, 0, 0, 0, 1, 2, 5, 0, 0, 0)))
  }

  "release the reopened 10 bytes file" in {
    // root: 0, dir: 1, file: 2, dataId: 3
    assert(fs.release(tenBytesId) == Some(3))
  }

  "close write backend" in fs.shutdown()
