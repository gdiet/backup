package dedup

import java.io.{File, RandomAccessFile}

import util.Hash

import scala.util.Using.resource

object Store {
  private def hashAlgorithm = "SHA-1"

  def run(options: Map[String, String]): Unit = {
    // Note: Implemented for single-threaded operation.
    val (repo, source) = (options.get("repo"), options.get("source")) match {
      case (None, None) => throw new IllegalArgumentException("One of source or repo option is mandatory.")
      case (repOpt, sourceOpt) => new File(repOpt.getOrElse("")).getAbsoluteFile -> new File(sourceOpt.getOrElse("")).getAbsoluteFile
    }
    require(source.exists(), "Source does not exist.")
    require(source.getParent != null, "Can't store root.")

    val dbDir = Database.dbDir(repo)
    val ds = new DataStore(repo, readOnly = false)
    if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
    resource(util.H2.rw(dbDir)) { connection =>
      val fs = new StoreFS(connection)

      val refId = options.get("reference").map { refPath =>
        fs.globDir(refPath).getOrElse(throw new IllegalArgumentException(s"Reference directory $refPath does not exist."))
      }

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))
      require(!fs.exists(targetId, source.getName), "Can't overwrite in target.")

      def walk(parent: Long, file: File, refId: Option[Long]): Unit = if (file.isDirectory) {
        val id = fs.mkEntry(parent, file.getName, None, None)
        val newRef = refId.flatMap(fs.child(_, file.getName)).flatMap(_.asDir)
        progressMessage(s"Storing dir $file")
        newRef.foreach(r => println(s"Reference $r for dir $file")) // FIXME remove
        file.listFiles().foreach(walk(id, _, newRef))
      } else resource(new RandomAccessFile(file, "r")) { ra =>
        progressMessage(s"Storing file $file")
        val (hash, size) = Hash(hashAlgorithm, read(ra))(_.map(_.length.toLong).sum)
        val dataId = fs.dataEntry(hash, size).getOrElse {
          val start = fs.startOfFreeData
          val (newHash, stop) = Hash(hashAlgorithm, read(ra.tap(_.seek(0))))(_.foldLeft(start) {
            case (pos, chunk) => ds.write(pos, chunk); pos + chunk.length
          })
          fs.mkEntry(start, stop, newHash).tap(_ => fs.dataWritten(size))
        }
        fs.mkEntry(parent, file.getName, Some(file.lastModified), Some(dataId))
      }

      val details = s"$source at $targetPath in repository $repo"
      println(s"Storing $details")
      walk(targetId, source, refId)
      resource(connection.createStatement)(_.execute("SHUTDOWN COMPACT"))
      println(s"Finished storing $details")
    }
  }

  private var lastProgressMessageAt = System.currentTimeMillis
  def progressMessage(message: String): Unit =
    if (System.currentTimeMillis - lastProgressMessageAt >= 5000) {
      lastProgressMessageAt = System.currentTimeMillis
      println(message)
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
