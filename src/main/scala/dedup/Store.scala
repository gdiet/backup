package dedup

import java.io.{File, RandomAccessFile}

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
    val ds = new Datastore(Datastore.datastoreDir(repo))
    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))
      require(!fs.exists(targetId, source.getName), "Can't overwrite in target.")

      println(s"Storing $source in repository $repo")
      def walk(parent: Long, file: File): Unit = if (file.isDirectory) {
        val id = fs.mkEntry(parent, file.getName, None, None)
        println(s"Created dir $file")
        file.listFiles().foreach(walk(id, _))
      } else {
        val startPosition = 200L // FIXME get actual start position
        resource(new RandomAccessFile(file, "r")) { ra =>
          val bytes = new Array[Byte](10000000)
          var endPosition = startPosition
          var bytesRead = 0
          while({bytesRead = ra.read(bytes); bytesRead > 0}) {
            ds.write(endPosition, bytes.take(bytesRead))
            endPosition += bytesRead
          }
        }
      }

      walk(targetId, source)
    }
  }
}
