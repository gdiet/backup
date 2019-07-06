package dedup

import dedup.Database.dbDir

import scala.util.Using.resource

object Init extends App {
  run(Map())

  def run(options: Map[String, String]): Unit = {
    if (dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir already exists.")
    resource(util.H2.rw(dbDir)) { connection =>
      Database.initialize(connection)
    }
  }
}
