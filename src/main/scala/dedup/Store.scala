package dedup

import dedup.Database.dbDir

import scala.util.Using.resource

object Store extends App {
  run(Map(
    "source" -> """e:\georg\temp\sub""",
    "target" -> "/dir/"
  ))

  def run(options: Map[String, String]): Unit = {
    val sourcePath = options.getOrElse("source", throw new IllegalArgumentException("source option is mandatory."))
    val source = new java.io.File(sourcePath)
    require(source.exists(), "Source does not exist.")

    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      fs.entryAt(targetPath).foreach {
        case "dir" => require(fs.entryAt(s"$targetPath/${source.getName}").isEmpty, "Can't overwrite existing entries in target.")
        case other => throw new IllegalStateException(s"Target is a $other, not a directory.")
      }
    }
  }
}
