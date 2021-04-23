package dedup

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource
import cache.LongDecorator

object XTestLevel1 extends App with ClassLogging {
  implicit object BytesSink extends DataSink[Array[Byte]] {
    override def write(dataSink: Array[Byte], offset: Long, data: Array[Byte]): Unit =
      System.arraycopy(data, 0, dataSink, offset.asInt, data.length)
  }

  val baseFolder = new File("dedupfs-temp")

  log.info(s"Clean up")
  CleanInit.delete(baseFolder)
  try {
    log.info(s"Initialize database")
    val repo = new File(baseFolder, "repo").getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    resource(H2.file(dbDir, readonly = false))(Database.initialize)

    log.info(s"Create store")
    val temp = new File(baseFolder + "temp")
    val settings = Settings(repo, dbDir, temp, readonly = false, new AtomicBoolean(false))
    resource(new Level1(settings)) { store =>

      log.info(s"Create a file.")
      store.createAndOpen(parentId = 0, name = "file", time = 0).get

      def file = {
        store.child(0, "file").get.asInstanceOf[FileEntry].tap { f =>
          log.info(s"Access " + f)
        }
      }
      val fileId = file.id

      log.info(s"Write 1000 bytes.")
      store.write(fileId, LazyList(0L -> Array.fill(1000)(Array[Byte](70, 71, 72, 73, 74)).flatten))

      log.info(s"Close the file.")
      store.release(fileId)
      Thread.sleep(100) // Wait until persisted

      log.info(s"Open the file.")
      store.open(file)

      log.info(s"Write 6 bytes at position 2.")
      store.write(fileId, LazyList(2L -> Array.fill(6)(80)))

      log.info(s"Read 10 bytes.")
      val bytesRead = new Array[Byte](10)
      store.read(fileId, 0, 10, bytesRead)

      log.info(s"Check read result " + bytesRead.toList)
      require(bytesRead.sameElements(Array[Byte](70, 71, 80, 80, 80, 80, 80, 80, 73, 74)))

      log.info(s"Close the file.")
      store.release(fileId)
      Thread.sleep(100) // Wait until persisted

      log.info(s"Close file system.")
    }
  } finally {
    log.info(s"Clean up")
    CleanInit.delete(baseFolder)
  }
}
