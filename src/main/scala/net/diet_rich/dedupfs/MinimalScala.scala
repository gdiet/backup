package net.diet_rich.dedupfs

import java.nio.file.Paths
import java.util.Objects

import jnr.ffi.Platform
import jnr.ffi.Platform.OS.WINDOWS
import ru.serce.jnrfuse.{ErrorCodes, FuseStubFS}
import ru.serce.jnrfuse.struct.{FileStat, Statvfs}

object MinimalScala extends App {
  val fs = new MinimalScala
  try {
    val path = Platform.getNativePlatform.getOS match {
      case WINDOWS => "J:\\"
      case _       => "/tmp/mntm"
    }
    fs.mount(Paths.get(path), true, false)
  } finally fs.umount()
}

class MinimalScala extends FuseStubFS {
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
    super.statfs(path, stbuf)
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
}
