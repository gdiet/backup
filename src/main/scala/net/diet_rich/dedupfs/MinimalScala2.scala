package net.diet_rich.dedupfs

import java.nio.file.Paths
import java.util.Objects

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import net.diet_rich.util.Log
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}

object MinimalScala2 extends App {
  def log: Log.type = Log

  def mount(): AutoCloseable = {
    new AutoCloseable {
      val fuseFS = new MinimalScala2()
      try {
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => "J:\\"
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

class MinimalScala2 extends FuseStubFS {
  override def statfs(path: String, stbuf: Statvfs): Int = {
    println("statfs " + path)
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
    0
  }

  override def getattr(path: String, stat: FileStat): Int = {
    System.out.println("getattr " + path)
    if (Objects.equals(path, "/")) {
      stat.st_mode.set(FileStat.S_IFDIR | 493) // 0755
      stat.st_nlink.set(2)
      0
    }
    else -ErrorCodes.ENOENT
  }

  override def readdir(path: String, buf: Pointer, filter: FuseFillDir, offset: Long, fi: FuseFileInfo): Int = {
    System.out.println("readdir " + path)
    if ("/" != path) -ErrorCodes.ENOENT else {
      filter.apply(buf, ".", null, 0)
      filter.apply(buf, "..", null, 0)
      0
    }
  }
}
