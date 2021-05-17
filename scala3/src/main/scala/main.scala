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

given scala.util.CommandLineParser.FromString[File] with
  def fromString(file: String) = File(file).getAbsoluteFile()

@main def init(repo: File) =
  val dbDir = db.dbDir(repo)
  if dbDir.exists() then
    main.failureExit(s"Database directory $dbDir exists - repository is probably already initialized.");
  resource(db.H2.connection(dbDir, readonly = false))(db.initialize)
  main.exit(s"Database initialized for repository $repo.")

// TODO for param file = File(repoDir).getAbsoluteFile(), use FromString type class
@main def mount(repo: File) =
  ???
