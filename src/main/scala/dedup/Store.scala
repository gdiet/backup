package dedup

import java.io.{File, RandomAccessFile}

import util.Hash

import scala.util.Using.resource

object Store extends App {
  run(Map(
    "source" -> """e:\georg\privat\dev\backup\src""",
    "target" -> "/backup/v1"
  ))

  def run(options: Map[String, String]): Unit = {
    val (repo, source) = (options.get("repo"), options.get("source")) match {
      case (None, None) => throw new IllegalArgumentException("One of source or repo option is mandatory.")
      case (repOpt, sourceOpt) => new File(repOpt.getOrElse(".")).getAbsoluteFile -> new File(sourceOpt.getOrElse(".")).getAbsoluteFile
    }
    require(source.exists(), "Source does not exist.")
    require(source.getParent != null, "Can't store root.")

    val dbDir = Database.dbDir(repo)
    val ds = new Datastore(Datastore.datastoreDir(repo), readOnly = false)
    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))
      require(!fs.exists(targetId, source.getName), "Can't overwrite in target.")

      def walk(parent: Long, file: File): Unit = if (file.isDirectory) {
        val id = fs.mkEntry(parent, file.getName, None, None)
        println(s"Created dir $id -> $file")
        file.listFiles().foreach(walk(id, _))
      } else {
        resource(new RandomAccessFile(file, "r")) { ra =>
          val (size, hash) = Hash(Datastore.hashAlgorithm, read(ra))
          val dataId = fs.dataEntry(hash, size).getOrElse {
            val start = fs.startOfFreeData // FIXME must not be run in parallel
            val stop = read(ra.tap(_.seek(0))).foldLeft(start) { // FIXME re-calculate hash
              case (pos, chunk) => ds.write(pos, chunk); pos + chunk.length
            }
            val dataId = fs.mkEntry(start, stop, hash).tap(_ => fs.dataWritten(size))
            val id = fs.mkEntry(parent, file.getName, Some(file.lastModified), Some(dataId))
            println(s"Created file $id -> $file")
          }
        }
      }

      println(s"Storing $source in repository $repo")
      walk(targetId, source)
    }
  }

  private def read(ra: RandomAccessFile): LazyList[Array[Byte]] =
    LazyList.unfold(()){ _ =>
      val chunkSize = 1000000
      val bytes = new Array[Byte](chunkSize)
      val read = ra.read(bytes, 0, chunkSize)
      if (read <= 0) None
      else if (read == chunkSize) Some(bytes -> ())
      else Some(bytes.take(read) -> ())
    }

  private def readWholeFile(ra: RandomAccessFile, maxSize: Int): Option[Array[Byte]] = {
    val size = ra.length
    Option.when(size <= maxSize)(new Array[Byte](size.toInt).tap(ra.readFully))
  }
}
