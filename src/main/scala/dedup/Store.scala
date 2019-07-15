package dedup

import java.io.{File, RandomAccessFile}

import dedup.Database.{DirNode, FileNode}
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
      val fs = new MetaFS(connection)

      val referenceDir = options.get("reference").map { refPath =>
        fs.globEntry(refPath).pipe {
          case None => throw new IllegalArgumentException(s"Reference directory $refPath does not exist.")
          case Some(_: FileNode) => throw new IllegalArgumentException(s"Reference $refPath is a file, not a directory.")
          case Some(dir: DirNode) => dir
        }
      }

      val targetPath = options.getOrElse("target", throw new IllegalArgumentException("target option is mandatory."))
      val targetId = fs.mkDirs(targetPath).getOrElse(throw new IllegalArgumentException("Can't create target directory."))
      require(!fs.exists(targetId, source.getName), "Can't overwrite in target.")

      def walk(parent: Long, file: File, referenceDir: Option[DirNode]): Unit = if (file.isDirectory) {
        progressMessage(s"Storing dir $file")
        val newRef = referenceDir.flatMap(dir => fs.child(dir.id, file.getName).collect { case child: DirNode => child })
        val id = fs.mkEntry(parent, file.getName, None, None)
        file.listFiles().foreach(walk(id, _, newRef))
      } else resource(new RandomAccessFile(file, "r")) { ra =>
        progressMessage(s"Storing file $file")
        val newRef = referenceDir.flatMap(dir => fs.child(dir.id, file.getName).collect { case child: FileNode => child })
        val dataId = newRef match {
          case Some(ref) if ref.lastModified == file.lastModified && ref.size == file.length => ref.dataId
          case _ =>
            val cacheSize = 50000000
            val cachedBytes = read(ra, cacheSize).force // MUST be 'val'
            def allBytes = cachedBytes #::: read(ra) // MUST be 'def'
            val (hash, size) = Hash(hashAlgorithm, allBytes)(_.foldLeft(0L)(_ + _.length))
            fs.dataEntry(hash, size).getOrElse {
              ra.seek(cacheSize) // Side effect on allBytes
              val start = fs.startOfFreeData
              val (newHash, stop) = Hash(hashAlgorithm, allBytes)(_.foldLeft(start) {
                case (pos, chunk) => ds.write(pos, chunk); pos + chunk.length
              })
              fs.mkEntry(start, stop, newHash).tap(_ => fs.dataWritten(size))
            }
        }
        fs.mkEntry(parent, file.getName, Some(file.lastModified), Some(dataId))
      }

      val details = s"$source at $targetPath in repository $repo"
      println(s"Storing $details")
      walk(targetId, source, referenceDir)
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

  private def read(ra: RandomAccessFile, size: Long = Long.MaxValue): LazyList[Array[Byte]] =
    LazyList.unfold(size){ remainingSize =>
      if (remainingSize <= 0) None else {
        val currentChunkSize = math.min(1000000, remainingSize).toInt
        val chunk = new Array[Byte](currentChunkSize)
        val read = ra.read(chunk)
        if (read < 0) None
        else if (read == currentChunkSize) Some(chunk -> (remainingSize - read))
        else Some(chunk.take(read) -> (remainingSize - read))
      }
    }
}
