package dedup

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

object XTestLevel1 extends App with ClassLogging {

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
    val store = new Level1(settings)

    log.info(s"Create a file.")
    store.createAndOpen(parentId = 0, name = "file", time = 0).get

    log.info(s"Access the file.")
    val file = store.child(0, "file").get

    log.info(s"Write 1000 bytes.")
    store.write(file.id, LazyList(0L -> Array.fill(1000)(65)))

    log.info(s"Close the file.")
    store.release(file.id)

    log.info(s"Close file system.")
    store.close()
  } finally {
    log.info(s"Clean up")
    CleanInit.delete(baseFolder)
  }
}
