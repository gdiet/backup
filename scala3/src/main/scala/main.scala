package dedup

import java.io.File
import scala.util.Using.resource

object main extends util.ClassLogging {
  def failureExit(msg: String*): Nothing =
    msg.foreach(log.error(_))
    Thread.sleep(200)
    sys.exit(1)
  def exit(msg: String*): Nothing =
    msg.foreach(log.info(_))
    Thread.sleep(200)
    sys.exit(0)
}

// TODO for param file = File(repoDir).getAbsoluteFile(), use FromString type class
@main def init(repoDir: String) =
  val repo  = File(repoDir).getAbsoluteFile()
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then
    main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.");
  resource(db.H2.connection(dbDir, readonly = false))(db.initialize)
  main.exit(s"Database initialized for repository $repo.")
