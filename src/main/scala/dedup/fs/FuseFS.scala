package dedup.fs
import java.nio.file.Paths

import scala.io.StdIn

import dedup.fs.util.chaining
import jnr.ffi.Pointer
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object FuseFS extends App {
  val fs = new FuseFS
  fs.mount(Paths.get("""I:\"""))
  StdIn.readLine("[enter] to exit ...")
  fs.umount()
}

object util {
  // see https://github.com/dwijnand/scala/blob/278828b9a2965470e8cbe08a10780b5e923c4c11/src/library/scala/util/ChainingOps.scala
  implicit class chaining[A](private val self: A) extends AnyVal {
    def tap[U](f: A => U): A = { f(self); self }
  }
}

/** Stage 1: For a minimal (empty) file system, implement getattr & readdir. These two functions also suffice for
  * read-only directory browsing without files. */
class FuseFS extends FuseStubFS {
  val O777     : Int = 511 // octal 0777
  val OK       : Int = 0
  val ENOENT   : Int = -ErrorCodes.ENOENT
  val EOVERFLOW: Int = -ErrorCodes.EOVERFLOW

  def log[T](message: => String, asTrace: Boolean = false)(f: => T): T =
    try f.tap(result => if (!asTrace) synchronized(println(s"$message -> $result")))
    catch { case e: Throwable => synchronized { println(s"$message -> $e"); e.printStackTrace() }; throw e }
  def trace[T](message: => String)(f: => T): T = log(message, asTrace = true)(f)

  /** Return file attributes, i.e., for the given path, fill in the elements of the "stat" structure. */
  // TODO check see https://linux.die.net/man/2/stat
  // TODO check https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
  // Note: Calling FileStat.toString DOES NOT WORK, there's a PR: https://github.com/jnr/jnr-ffi/pull/176
  override def getattr(path: String, stat: FileStat): Int = trace(s"getattr($path, fileStat)") {
    stat.st_uid.set(getContext.uid.get)
    stat.st_gid.set(getContext.gid.get)
    path match {
      case "/" => stat.st_mode.set(FileStat.S_IFDIR | O777); OK
//      case "/hello.txt" =>
//        stat.st_mode.set(FileStat.S_IFREG | O777)
//        stat.st_size.set(fileSize)
      case _ => ENOENT
    }
  }

  /** See https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details */
  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    log(s"readdir($path, buffer, filler, $offset, fileInfo)") {
      if (offset.toInt < 0 || offset.toInt != offset) EOVERFLOW
      else path match {
        case "/" =>
          def entries = "." #:: ".." #:: Stream.empty[String]
          entries.zipWithIndex
            .drop(offset.toInt)
            .exists { case (entry, k) => filler.apply(buf, entry, null, k + 1) != 0 }
          OK
        case _ => ENOENT // TODO distinguish from ENOTDIR
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
