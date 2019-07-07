package dedup

import java.io.File

import scala.util.Using.resource

object Init extends App {
  run(Map())

  def run(options: Map[String, String]): Unit = {
    val repo = new File(options.getOrElse("repo", ".")).getAbsoluteFile
    val dbDir = Database.dbDir(repo)
    println(s"Initializing repository at $repo")
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(util.H2.rw(dbDir)) { connection =>
      Database.initialize(connection)
    }
  }
}
