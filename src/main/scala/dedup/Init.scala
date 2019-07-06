package dedup

import java.io.File
import scala.util.Using.resource
import util.H2

object Init extends App {
  run(Map())

  def run(options: Map[String, String]): Unit = {
    val dbdir = new File("./fsdb")
    if (dbdir.exists()) throw new IllegalStateException("fsdb already exists.")
    resource(H2(dbdir)) { connection =>
      Database.initialize(connection)
    }
  }
}
