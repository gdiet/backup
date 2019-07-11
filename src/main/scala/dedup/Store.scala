package dedup

import java.io.{File, RandomAccessFile}

import util.Hash

import scala.util.Using.resource

object Store {
  private def hashAlgorithm = "SHA-1"

  def run(options: Map[String, String]): Unit = {
    // Note: Implemented strictly for single-threaded use
    val (repo, source) = (options.get("repo"), options.get("source")) match {
      case (None, None) => throw new IllegalArgumentException("One of source or repo option is mandatory.")
      case (repOpt, sourceOpt) => new File(repOpt.getOrElse(".")).getAbsoluteFile -> new File(sourceOpt.getOrElse(".")).getAbsoluteFile
    }
    require(source.exists(), "Source does not exist.")
    require(source.getParent != null, "Can't store root.")

    val dbDir = Database.dbDir(repo)
    val ds = new DataStore(repo, readOnly = false)
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
          val (hash, size) = Hash(hashAlgorithm, read(ra))(_.map(_.length.toLong).sum)
          val dataId = fs.dataEntry(hash, size).getOrElse {
            val start = fs.startOfFreeData
            val (newHash, stop) = Hash(hashAlgorithm, read(ra.tap(_.seek(0))))(_.foldLeft(start) {
              case (pos, chunk) => ds.write(pos, chunk); pos + chunk.length
            })
            fs.mkEntry(start, stop, newHash).tap(_ => fs.dataWritten(size))
          }
          val id = fs.mkEntry(parent, file.getName, Some(file.lastModified), Some(dataId))
          println(s"Created file $id / $dataId -> $file")
        }
      }

      println(s"Storing $source in repository $repo")
      walk(targetId, source)
      resource(connection.createStatement)(_.execute("SHUTDOWN COMPACT"))
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
}
