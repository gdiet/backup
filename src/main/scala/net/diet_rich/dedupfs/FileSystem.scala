package net.diet_rich.dedupfs

import jnr.ffi.Platform
import jnr.ffi.Platform.OS.WINDOWS
import net.diet_rich.util._

object FileSystem extends ClassLogging with App {
  def mount(fs: SqlFS): AutoCloseable = try {
    // TODO Eventually, we want to have the mount point configurable.
    val mountPoint = Platform.getNativePlatform.getOS match {
      case WINDOWS => """I:\"""
      case _ => "/tmp/mntfs"
    }
    log.info(s"mount($mountPoint)")
    fs.mount(java.nio.file.Paths.get(mountPoint), false, false)

    // FIXME remove
    println(fs.mkdir("/hallo", 0))
    println(fs.mkdir("/hallo/welt", 0))
    println(fs.mkdir("/hello", 0))
    println(fs.mkdir("/hello/world", 0))
    println(fs.readdir("/").asInstanceOf[fs.ReaddirOk].children.mkString("\n"))

    () => fs.umount()
  } catch { case e: Throwable =>
    log.info(e.getMessage)
    fs.umount(); throw e
  }

  val fsHandle = mount(new SqlFS)
  try io.StdIn.readLine("[enter] to exit ...")
  finally fsHandle.close()
}
