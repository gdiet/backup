package dedup.fuse
import java.nio.file.Paths

import scala.io.StdIn
import scala.util.chaining.scalaUtilChainingOps

import dedup.fs._
import jnr.ffi.Pointer
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object FuseFS extends App {
  val fs = new FuseFS
  fs.mount(Paths.get("""I:\"""))
  StdIn.readLine("[enter] to exit ...\n")
  fs.umount()
}

/** Stage 1: For a minimal (empty) file system, implement getattr & readdir.
  * Stage 2: These two functions also suffice for read-only directory browsing without files. */
class FuseFS extends FuseStubFS {
  val fs = new FS

  val O777     : Int = 511 // octal 0777
  val OK       : Int = 0
  val ENOTDIR  : Int = -ErrorCodes.ENOTDIR
  val ENOENT   : Int = -ErrorCodes.ENOENT
  val EOVERFLOW: Int = -ErrorCodes.EOVERFLOW

  def warn(message: => String): Unit = println(s"!!! $message")
  def info[T](message: => String, asTrace: Boolean = false)(f: => T): T =
    try f.tap(result => if (!asTrace) synchronized(println(s"$message -> $result")))
    catch { case e: Throwable => synchronized { println(s"$message -> $e"); e.printStackTrace() }; throw e }
  def trace[T](message: => String)(f: => T): T = info(message, asTrace = true)(f)

  def split(path: String): Seq[String] =
    if (path.startsWith("/")) path.split("/").drop(1).toSeq
    else { warn(s"Unexpected path $path"); path.split("/").toSeq }

  /** Return file attributes, i.e., for the given path, fill in the elements of the "stat" structure. */
  override def getattr(path: String, stat: FileStat): Int = trace(s"getattr($path, fileStat)") {
    // Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176
    // TODO check https://linux.die.net/man/2/stat
    // TODO check https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
    // TODO for block size and block number, see https://github.com/Alluxio/alluxio/blob/master/integration/fuse/src/main/java/alluxio/fuse/AlluxioFuseFileSystem.java
    stat.st_uid.set(getContext.uid.get)
    stat.st_gid.set(getContext.gid.get)
    fs.info(split(path)) match {
      case None => ENOENT
      case Some(EntryInfo(_, None)) =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        OK
      case Some(EntryInfo(_, Some(FileInfo(size, time)))) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_size.set(size)
        stat.st_mtim.tv_sec.set(time / 1000)
        stat.st_mtim.tv_nsec.set((time % 1000) * 1000)
        OK
    }
  }

  /** See https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
    *
    * Note: At least for Windows, there is no performance benefit in implementing opendir/releasedir and handing over
    * the file handle to readdir, because opendir/releasedir are called many times as often as readdir. */
  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    info(s"readdir($path, buffer, filler, $offset, fileInfo)") {
      if (offset.toInt < 0 || offset.toInt != offset) EOVERFLOW
      else {
        fs.list(split(path)) match {
          case NotFound => ENOENT
          case IsFile => ENOTDIR
          case DirEntries(content) =>
            def entries = "." #:: ".." #:: content
            entries.zipWithIndex
              .drop(offset.toInt)
              .exists { case (entry, k) => filler.apply(buf, entry, null, k + 1) != 0 }
            OK
        }
      }
    }

//  // TODO check https://linux.die.net/man/2/statfs
//  // TODO check https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
//  override def statfs(path: String, stbuf: Statvfs): Int = log(s"statfs($path, buffer)") {
//    // statfs needs to be implemented on Windows in order to allow for copying data from other devices
//    // because winfsp calculates the volume size based on the statvfs call.
//    // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
//    // TODO Eventually, implement meaningful numbers here.
//    stbuf.f_blocks.set(1000000)    // total data blocks in fs
//    stbuf.f_frsize.set(1024)       // fs block size
//    stbuf.f_bfree.set(1024 * 1024) // free blocks in fs
//    OK
//  }
}
