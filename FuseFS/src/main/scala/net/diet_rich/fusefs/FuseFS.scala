package net.diet_rich.fusefs

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import net.diet_rich.scalafs._
import net.diet_rich.util._
import net.diet_rich.util.fs._
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

class FuseFS(fs: FileSystem) extends FuseStubFS with ClassLogging { import FuseFS._

  // Note: Calling FileStat.toString DOES NOT WORK
  override def getattr(path: String, stat: FileStat): Int = log(s"getattr($path, fileStat)") {
    stat.st_uid.set(getContext.uid.get)
    stat.st_gid.set(getContext.gid.get)
    fs.getNode(path) match {
      case None => ENOENT
      case Some(_: Dir) =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        OK
      case Some(file: File) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_size.set(file.size)
        OK
    }
  }

  override def mkdir(path: String, mode: Long): Int = log(s"mkdir($path, $mode)") {
    fs.getNode(path) match {
      case Some(_) => EEXIST
      case None => FileSystem.splitParentPath(path).flatMap {
        case (parent, name) => fs.getNode(parent).collect { case dir: Dir => dir.mkDir(name) }
      } match {
        case Some(true) => 0
        case _ => ENOENT
      }
    }
  }

  // see also https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
  override def readdir(path: String,
                       buf: Pointer,
                       filler: FuseFillDir,
                       offset: Long,
                       fi: FuseFileInfo): Int = log(s"readdir($path, buffer, filler, $offset, fileInfo)") {
    if (offset.toInt < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW
    else fs.getNode(path) match {
      case None => ENOENT
      case Some(_: File) => ENOTDIR
      case Some(dir: Dir) =>
        def entries = "." #:: ".." #:: dir.list.toStream.map(_.name)
        entries.zipWithIndex
          .drop(offset.toInt)
          .exists {
            case (entry, k) => filler.apply(buf, entry, null, k + 1) != 0
          }
        OK
    }
  }

  override def rename(oldpath: String, newpath: String): Int = log(s"rename($oldpath, $newpath)") {
    fs.getNode(oldpath) match {
      case None => ENOENT
      case Some(node) => node.renameTo(newpath) match {
        case RenameOk => OK
        case TargetExists => EEXIST
        case TargetParentDoesNotExist => ENOENT
        case TargetParentNotADirectory => ENOTDIR
      }
    }
  }

  override def rmdir(path: String): Int = log(s"rmdir($path)") {
    fs.getNode(path) match {
      case None => ENOENT
      case Some(_: File) => ENOTDIR
      case Some(dir: Dir) =>
        dir.delete() match {
          case DeleteOk => OK
          case DeleteHasChildren => ENOTEMPTY
          case DeleteNotFound => ENOENT
        }
    }
  }

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

  override def unlink(path: String): Int = log(s"unlink($path)") {
    fs.getNode(path) match {
      case None => ENOENT
      case Some(_: Dir) => EISDIR
      case Some(file: File) =>
        file.delete() match {
          case DeleteOk => OK
          case DeleteHasChildren => EIO
          case DeleteNotFound => ENOENT
        }
    }
  }
}

object FuseFS extends ClassLogging {
  private val O777      = 511 // octal 0777
  private val OK        = 0
  private val EEXIST    = -ErrorCodes.EEXIST
  private val EIO       = -ErrorCodes.EIO
  private val EISDIR    = -ErrorCodes.EISDIR
  private val ENOENT    = -ErrorCodes.ENOENT
  private val ENOTDIR   = -ErrorCodes.ENOTDIR
  private val ENOTEMPTY = -ErrorCodes.ENOTEMPTY

  def mount(fs: FileSystem): AutoCloseable = {
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
}
