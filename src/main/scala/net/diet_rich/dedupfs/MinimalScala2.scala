package net.diet_rich.dedupfs

import java.nio.file.Paths
import java.util.Objects

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import net.diet_rich.util.{ClassLogging, Log}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object MinimalScala2 extends App with ClassLogging {
  def mount(): AutoCloseable = {
    new AutoCloseable {
      val fuseFS = new MinimalScala2()
      try {
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => "I:\\"
          case _ => "/tmp/mntfs"
        }
        log.info(s"mount($mountPoint)")
        fuseFS.mount(Paths.get(mountPoint), false, false)
      } catch { case e: Throwable =>
        log.info(e.getMessage)
        fuseFS.umount(); throw e
      }
      override def close(): Unit = fuseFS.umount()
    }
  }

  val fuseFS = mount()
  try io.StdIn.readLine("[enter] to exit ...")
  finally fuseFS.close()
}

class MinimalScala2 extends FuseStubFS with ClassLogging {
  private val OK = 0
  private val O777 = 511 // octal 0777

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

  override def getattr(path: String, stat: FileStat): Int = {
    log.info(s"getattr($path, fileStat)")
    if (Objects.equals(path, "/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 493) // 0755
      stat.st_nlink.set(2)
      0
    }
    else -ErrorCodes.ENOENT
  }

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = {
    log.info("readdir " + path)
    if ("/" != path) -ErrorCodes.ENOENT else {
      filter.apply(buf, ".", null, 0)
      filter.apply(buf, "..", null, 0)
      0
    }
  }
}
