package dedup

import java.nio.file.Files

import dedup.Database.dbDir

import scala.jdk.StreamConverters._
import scala.util.Using.resource

object Store extends App {
  run(Map(
    "source" -> """e:\georg\privat\dev\backup\src\""",
    "target" -> "/a/b/c"
  ))

  def run(options: Map[String, String]): Unit = {
    val sourcePath = options.getOrElse("source", throw new IllegalArgumentException("source option is mandatory."))
    val source = new java.io.File(sourcePath).getAbsoluteFile
    require(source.exists(), "Source does not exist.")
    val sourceParent = Option(source.getParent).getOrElse(throw new IllegalArgumentException("Can't store root."))

    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))

      Files.walk(source.toPath).toScala(LazyList).foldLeft(targetId) { case (id, path) =>
        val sourceFile = path.toFile
        val targetFile = targetPath + sourceFile.toString.drop(sourceParent.length).replaceAll("\\\\", "/")
        println(s"$sourceFile -> $targetFile")
        if (sourceFile.isDirectory) {

        } else {

        }
        id
      }
    }
  }
}
