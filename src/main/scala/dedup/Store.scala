package dedup

import java.io.{File, RandomAccessFile}
import java.security.MessageDigest

import dedup.Database.{DirNode, FileNode}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Using.resource

object Store {
  private def hashAlgorithm = "MD5"

  def run(options: Map[String, String]): Unit = {
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

      import scala.concurrent.ExecutionContext.Implicits.global
      def walk(parent: Long, file: File, referenceDir: Option[DirNode]): Future[(Long, Long, Long)] = if (file.isDirectory) {
        progressMessage(s"Storing dir $file")
        val newRef = referenceDir.flatMap(dir => sync(fs.child(dir.id, file.getName)).collect { case child: DirNode => child })
        val id = sync(fs.mkEntry(parent, file.getName, None, None))
        Future.sequence(file.listFiles().toSeq.map(walk(id, _, newRef))).map { s =>
          def combine(a: (Long, Long, Long), b: (Long, Long, Long)) = (a._1 + b._1, a._2 + b._2, a._3 + b._3)
          combine((1, 0, 0), s.foldLeft[(Long, Long, Long)]((0, 0, 0))(combine))
        }
      } else resource(new RandomAccessFile(file, "r")) { ra =>
        progressMessage(s"Storing file $file")
        val newRef = referenceDir.flatMap(dir => sync(fs.child(dir.id, file.getName)).collect { case child: FileNode => child })
        val dataId = newRef match {
          case Some(ref) if ref.lastModified == file.lastModified && ref.size == file.length => ref.dataId
          case _ =>
            val md = MessageDigest.getInstance(hashAlgorithm)
            val cachedBytes = read(ra, 50000000) // must be 'val'
            val bytesCached = cachedBytes.map { chunk => md.update(chunk); chunk.length.toLong }.sum
            val mdClone = md.clone().asInstanceOf[MessageDigest]
            val fileSize = bytesCached + read(ra).map { chunk => md.update(chunk); chunk.length.toLong }.sum
            val hash = md.digest()
            sync(fs.dataEntry(hash, fileSize).getOrElse {
              val position = cachedBytes.foldLeft(fs.startOfFreeData) { case (pos, chunk) =>
                ds.write(pos, chunk); pos + chunk.length
              }
              ra.seek(bytesCached)
              val stop = read(ra).foldLeft(position) { case (pos, chunk) =>
                ds.write(pos, chunk); pos + chunk.length
              }
              fs.mkEntry(fs.startOfFreeData, stop, mdClone.digest()).tap(_ => fs.setStartOfFreeData(stop))
            })
        }
        sync(fs.mkEntry(parent, file.getName, Some(file.lastModified), Some(dataId)))
        Future.successful((0, 1, ra.length))
      }

      val referenceMessage = options.get("reference").fold("")(r => s" with reference $r")
      val details = s"$source at $targetPath$referenceMessage in repository $repo"
      println(s"Storing $details")
      val time = System.currentTimeMillis
      val (dirs, files, bytes) = Await.result(walk(targetId, source, referenceDir), Duration.Inf)
      resource(connection.createStatement)(_.execute("SHUTDOWN COMPACT"))
      println(s"Finished storing $details\n" +
        s"Stored $dirs directories, $files files, $bytes bytes in ${(System.currentTimeMillis() - time)/1000}s")
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
