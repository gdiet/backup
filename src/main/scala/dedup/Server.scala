package dedup

import java.io.File
import java.sql.Connection

import jnr.ffi.Pointer
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object Server {
  def run(options: Map[String, String]): Unit = {
    val mountPoint = options.getOrElse("mount", throw new IllegalArgumentException("mount option is mandatory."))
    val repo = new File(options.getOrElse("repo", "")).getAbsoluteFile
    println(s"Starting server for repository $repo")
    val fs = new Server(repo)
    try fs.mount(java.nio.file.Paths.get(mountPoint), true, false)
    finally { fs.umount(); println("Server stopped.") }
  }
}

class Server(repo: File) extends FuseStubFS {
  private val O777 = 511
  private val dbDir = Database.dbDir(repo)
  if (!dbDir.exists()) throw new IllegalStateException(s"Database directory $dbDir does not exist.")
  val connection: Connection = util.H2.ro(dbDir)
  private val ds = new DataStore(repo, readOnly = true)
  private val fs = new ServerFS(connection, ds)

  override def umount(): Unit = try super.umount() finally { ds.close(); fs.close() }

  /* Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176 */
  override def getattr(path: String, stat: FileStat): Int = {
    fs.entryAt(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FSDir) =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        stat.st_nlink.set(2)
        0
      case Some(file: FSFile) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_nlink.set(1)
        stat.st_size.set(file.size)
        stat.st_mtim.tv_nsec.set((file.lastModifiedMillis % 1000) * 1000)
        stat.st_mtim.tv_sec.set(file.lastModifiedMillis / 1000)
        0
    }
  }

  /* Note: No benefit expected in implementing opendir/releasedir and handing over the file handle to readdir. */
  override def readdir(path: String, buf: Pointer, fill: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    fs.entryAt(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FSFile) => -ErrorCodes.ENOTDIR
      case Some(dir: FSDir) =>
        if (offset < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
        else {
          def names = Seq(".", "..") ++ dir.childNames
          // exists: side effect until a condition is met
          names.zipWithIndex.drop(offset.toInt).exists { case (name, k) => fill.apply(buf, name, null, k + 1) != 0 }
          0
        }
    }

  override def open(path: String, fi: FuseFileInfo): Int =
    fs.entryAt(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FSDir) => -ErrorCodes.EISDIR
      case Some(_: FSFile) => 0
    }

  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int = {
    fs.entryAt(path) match {
      case None => -ErrorCodes.ENOENT
      case Some(_: FSDir) => -ErrorCodes.EISDIR
      case Some(file: FSFile) =>
        val intSize = size.toInt
        if (offset < 0 || size < 0 || intSize != size) -ErrorCodes.EOVERFLOW
        else {
          val bytes = file.bytes(offset, intSize)
          buf.put(0, bytes, offset.toInt, bytes.length)
          bytes.length
        }
    }
  }
}
