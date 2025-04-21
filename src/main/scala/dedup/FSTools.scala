package dedup

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Using.resource

object FSTools extends dedup.util.ClassLogging:

  def check(opts: Seq[(String, String)], path: String): Unit =
    val repo = opts.repo
    val temp = main.prepareTempDir(false, opts)
    val dbDir -> _ = main.prepareDbDir(repo, backup = false, readOnly = true)
    val settings = server.Settings(repo, dbDir, temp, readOnly = true, copyWhenMoving = AtomicBoolean(false))
    val (db, fs) = server.Backend.create(settings)
    resource(fs) { fs =>
      fs.entry(path) match
        case None =>
          println(s"DedupFS:$path does not exist.")
        case Some(dir: DirEntry) =>
          println(s"DedupFS:$path is a directory.")
        case Some(file: FileEntry) =>
          (db.logicalSize(file.dataId), db.storageSize(file.dataId), db.hash(file.dataId)) match
            case (0, 0, _) =>
              println("OK - file size 0.")
            case (logicalSize, 0, _) =>
              println(s"BLACKLISTED - file size $logicalSize.")
            case (logicalSize, storageSize, _) if logicalSize != storageSize =>
              println(s"BAD: Logical file size $logicalSize, storage size $storageSize.")
            case (size, _, None) =>
              println(s"BAD: Can't read hash from DB.")
            case (size, _, Some(hashInDb)) =>
              fs.open(file)
              try
                fs.read(file.id, 0, Long.MaxValue) match
                  case None =>
                    println(s"ERROR: Can't read file.")
                  case Some(data) =>
                    // Calculate hash
                    val md = java.security.MessageDigest.getInstance(hashAlgorithm)
                    data.foreach(entry => md.update(entry._2))
                    val hash = md.digest()
                    if (util.Arrays.equals(hash, hashInDb))
                      println(s"OK - file size $size.")
                    else
                      println("BAD - actual file hash is not as stored in DB.")
              finally
                fs.release(file.id)
    }
