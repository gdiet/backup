package dedup

import scala.util.Using.resource

object Init {
  def run(options: Map[String, String]): Unit = {
    val repo = options.fileFor("repo")
    val dbDir = Database.dbDir(repo)
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(util.H2.rw(dbDir)) { connection =>
      Database.initialize(connection)
    }
    println(s"Initialized repository at $repo")
  }
}
