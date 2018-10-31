package net.diet_rich.dedupfs

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import net.diet_rich.util._
import net.diet_rich.util.fs._
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

// FIXME this is such a thin wrapper that we can get rid of it (merge FuseFS and SqlFS)

object FuseConstants {
  val O777     : Int = 511 // octal 0777
  val OK       : Int = 0
  val EEXIST   : Int = -ErrorCodes.EEXIST
  val EIO      : Int = -ErrorCodes.EIO
  val EISDIR   : Int = -ErrorCodes.EISDIR
  val ENOENT   : Int = -ErrorCodes.ENOENT
  val ENOTDIR  : Int = -ErrorCodes.ENOTDIR
  val ENOTEMPTY: Int = -ErrorCodes.ENOTEMPTY
}

/** See for example https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html */
class FuseFS(fs: SqlFS) extends FuseStubFS with ClassLogging { import fs._
  import FuseConstants._

  // FIXME FUSE provides a "file handle" in the "fuse_file_info" structure. The file handle
  // FIXME is stored in the "fh" element of that structure, which is an unsigned 64-bit
  // FIXME integer (uint64_t) uninterpreted by FUSE. If you choose to use it, you should set
  // FIXME that field in your open, create, and opendir functions; other functions can then use it.

  /** Return file attributes. For the given pathname, this should fill in the elements of the "stat" structure.
    * We might want to fill in additional fields, see https://linux.die.net/man/2/stat and
    * https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html */
  // Note: Calling FileStat.toString DOES NOT WORK
  override def getattr(path: String, stat: FileStat): Int = log(s"getattr($path, fileStat)", asTrace = true) {
    stat.st_uid.set(getContext.uid.get)
    stat.st_gid.set(getContext.gid.get)
    getNode(path) match {
      case None => ENOENT
      case Some(_: Dir) => stat.st_mode.set(FileStat.S_IFDIR | O777); OK
      case Some(file: File) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_size.set(file.size)
        OK
    }
  }

  /** Attempts to create a directory named [path]. */
  override def mkdir(path: String, mode: Long): Int = log(s"mkdir($path, $mode)")(fs.mkdir(path, mode))

  /** See https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details */
  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int =
    log(s"readdir($path, buffer, filler, $offset, fileInfo)") {
      if (offset.toInt < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
      else fs.readdir(path) match {
        // ENOENT No such directory.
        // ENOTDIR path does not refer to a directory.
        case ReaddirBadPath => EIO
        case ReaddirNotFound => ENOENT
        case ReaddirNotADirectory => ENOTDIR
        case ReaddirOk(children) =>
          def entries = "." #:: ".." #:: children.toStream.map(_.name)
          entries.zipWithIndex
            .drop(offset.toInt)
            .exists {
              case (entry, k) => filler.apply(buf, entry, null, k + 1) != 0
            }
          OK
      }
    }

  /** Renames a file, moving it between directories if required. If newpath already exists it will be atomically
    * replaced. oldpath can specify a directory. In this case, newpath must either not exist, or it must specify
    * an empty directory. See https://linux.die.net/man/2/rename */
  override def rename(oldpath: String, newpath: String): Int = log(s"rename($oldpath, $newpath)") {
    fs.renameImpl(oldpath, newpath) match {
        // EINVAL An attempt was made to make a directory a subdirectory of itself.
        // EISDIR newpath is an existing directory, but oldpath is not a directory.
        // ENOENT The link named by oldpath does not exist; or, a directory component in newpath does not exist; or, oldpath or newpath is an empty string.
        // ENOTDIR A component used as a directory in oldpath or newpath is not, in fact, a directory. Or, oldpath is a directory, and newpath exists but is not a directory.
        // ENOTEMPTY or EEXIST newpath is a nonempty directory, that is, contains entries other than "." and "..".
        case RenameOk => OK
        case RenameNotFound => ENOENT
        case RenameTargetExists => EEXIST
        case RenameParentDoesNotExist => ENOENT
        case RenameParentNotADirectory => ENOTDIR
        case RenameBadPath => EIO
    }
  }

  /** Delete a directory. */ // TODO make sure implementation matches specification with regard to "." and ".."
  override def rmdir(path: String): Int = log(s"rmdir($path)") {
    fs.delete(path, expectDir = true) match {
      // EINVAL path has . as last component.
      // ENOENT A directory component in path does not exist.
      // ENOTDIR path, or a component used as a directory in pathname, is not, in fact, a directory.
      // ENOTEMPTY path contains entries other than . and .. ; or, path has .. as its final component.
      case DeleteOk => OK
      case DeleteHasChildren => ENOTEMPTY
      case DeleteNotFound => ENOENT
      case DeleteFileType => ENOTDIR
      case DeleteBadPath => EIO
    }
  }

  // FIXME check https://linux.die.net/man/2/statfs and https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
  override def statfs(path: String, stbuf: Statvfs): Int = log(s"statfs($path, buffer)") {
    if (Platform.getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying
      // data from other devices because winfsp calculates the volume size based
      // on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        // TODO Eventually, we want something meaningful here.
        stbuf.f_blocks.set(1024 * 1024) // total data blocks in file system
        stbuf.f_frsize.set(1024) // fs block size
        stbuf.f_bfree.set(1024 * 1024) // free blocks in fs
      }
    }
    OK
  }

  // FIXME check https://linux.die.net/man/2/unlink and https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html
  override def unlink(path: String): Int = log(s"unlink($path)") {
    fs.delete(path, expectDir = false) match {
      case DeleteOk => OK
      case DeleteHasChildren => EIO // should never happen
      case DeleteNotFound => ENOENT
      case DeleteFileType => EISDIR
      case DeleteBadPath => EIO
    }
  }

  // FIXME double-check
  override def create(path: String, mode: Long, fi: FuseFileInfo): Int = log(s"create($path, buf, $mode, fileInfo)") {
    fs.create(path) match {
      case CreateOk => OK
      case CreateIsDirectory => EISDIR
      case CreateNotFound => ENOENT
      case CreateBadPath => EIO
      case CreateBadParent => EIO
    }
  }

  // TODO probably not needed
  override def open(path: String, fi: FuseFileInfo): Int = log(s"open($path, fileInfo)") { OK }

  // FIXME double-check
  override def write(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    log(s"write($path, buf, $size, $offset, fileInfo)") {
      fs.write(path, buf, size, offset) match {
        case WriteOk => OK
        case WriteIsDirectory => EISDIR
        case WriteNotFound => ENOENT
        case WriteBadPath => EIO
      }
    }

  // FIXME double-check
  override def truncate(path: String, size: Long): Int = log(s"truncate($path, $size)") {
    fs.truncateImpl(path, size) match {
      case WriteOk => OK
      case WriteIsDirectory => EISDIR
      case WriteNotFound => ENOENT
      case WriteBadPath => EIO
    }
  }

  // FIXME double-check
  override def read(path: String, buf: Pointer, size: Long, offset: Long, fi: FuseFileInfo): Int =
    log(s"read($path, buf, $size, $offset, fileInfo)")(fs.read(path, buf, size, offset))
}

object FuseFS extends ClassLogging with App {
  def mount(fs: SqlFS): AutoCloseable = {
    new AutoCloseable {
      val fuseFS = new FuseFS(fs)
      try {
        // TODO Eventually, we want to have the mount point configurable.
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => """I:\"""
          case _ => "/tmp/mntfs"
        }
        log.info(s"mount($mountPoint)")
        fuseFS.mount(java.nio.file.Paths.get(mountPoint), false, false)
      } catch { case e: Throwable =>
        log.info(e.getMessage)
        fuseFS.umount(); throw e
      }
      override def close(): Unit = fuseFS.umount()
    }
  }

  val sqlFS = new SqlFS

  // FIXME remove
  import sqlFS._
  println(mkdir("/hallo", 0))
  println(mkdir("/hallo/welt", 0))
  println(mkdir("/hello", 0))
  println(mkdir("/hello/world", 0))
  println(readdir("/").asInstanceOf[ReaddirOk].children.mkString("\n"))

  val fsHandle = mount(sqlFS)
  try io.StdIn.readLine("[enter] to exit ...")
  finally fsHandle.close()
}
