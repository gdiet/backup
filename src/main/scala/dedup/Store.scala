package dedup

import java.io.File

import dedup.Database.dbDir

import scala.util.Using.resource

object Store extends App {
  run(Map(
    "source" -> """e:\georg\privat\dev\backup\src\""",
    "target" -> "/a/b/c"
  ))

  def run(options: Map[String, String]): Unit = {
    val (repo, source) = (options.get("repo"), options.get("source")) match {
      case (None, None) => throw new IllegalArgumentException("One of source or repo option is mandatory.")
      case (repOpt, sourceOpt) => new File(repOpt.getOrElse(".")).getAbsoluteFile -> new File(sourceOpt.getOrElse(".")).getAbsoluteFile
    }
    require(source.exists(), "Source does not exist.")
    require(source.getParent != null, "Can't store root.")

    val dbDir = Database.dbDir(repo)
    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))
      require(!fs.exists(targetId, source.getName), "Can't overwrite in target.")

      println(s"Starting server for repository $repo") // FIXME

      def walk(parent: Long, file: File): Unit = if (file.isDirectory) {
        val id = fs.mkEntry(parent, file.getName, None, None)
        println(s"made dir $file")
        file.listFiles().foreach(walk(id, _))
      } else {
      }

      walk(targetId, source)
    }
  }
}
