package net.diet_rich.fusefs

import jnr.ffi.{Platform, Pointer}
import jnr.ffi.Platform.OS.WINDOWS
import net.diet_rich.scalafs.{Dir, File, FileSystem}
import net.diet_rich.util.ClassLogging
import ru.serce.jnrfuse.examples.MemoryFS
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}

class FuseFS(fs: FileSystem) extends FuseStubFS with ClassLogging {
  private val OK = 0
  private val O777 = 511 // octal 0777

  override def getattr(path: String, stat: FileStat): Int = {
    log.info(s"getattr($path, fileStat)") // NOTE: calling FileState.toString DOES NOT WORK
    fs.getNode(path) match {
      case None             => -ErrorCodes.ENOENT
      case Some(_: Dir)     =>
        stat.st_mode.set(FileStat.S_IFDIR | O777)
        OK
      case Some(file: File) =>
        stat.st_mode.set(FileStat.S_IFREG | O777)
        stat.st_size.set(file.size)
        OK
    }
  }

  // see also https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201001/homework/fuse/fuse_doc.html#readdir-details
  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = {
    log.info(s"readdir($path, buffer, filler, $offset, $fi)")
    if (offset.toInt < 0 || offset.toInt != offset) -ErrorCodes.EOVERFLOW else
      fs.getNode(path) match {
        case None           => -ErrorCodes.ENOENT
        case Some(_: File)  => -ErrorCodes.ENOTDIR
        case Some(dir: Dir) =>
          def entries = "." #:: ".." #:: dir.list.toStream.map(_.name)

          entries
            .zipWithIndex
            .drop(offset.toInt)
            .takeWhile { case (entry, k) =>
              filler.apply(buf, entry, null, k + 1) == 0
            }
          OK
      }
  }

  override def statfs(path: String, stbuf: Statvfs): Int = {
    log.info(s"statfs($path, buffer)")
    if (Platform.getNativePlatform.getOS == WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying
      // data from other devices because winfsp calculates the volume size based
      // on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        stbuf.f_blocks.set(1024 * 1024) // total data blocks in file system
        stbuf.f_frsize.set(1024) // fs block size
        stbuf.f_bfree.set(1024 * 1024) // free blocks in fs
      }
    }
    OK
  }
}

object FuseFS extends ClassLogging {
  def mount(fs: FileSystem): AutoCloseable = {
    new AutoCloseable {
      val fuseFS = new FuseFS(fs)
      try {
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => "I:\\"
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
