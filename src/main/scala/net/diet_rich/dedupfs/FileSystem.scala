package net.diet_rich.dedupfs

import jnr.ffi.Platform
import jnr.ffi.Platform.OS.WINDOWS
import net.diet_rich.util._

object FileSystem extends ClassLogging with App {
  def mount(fs: SqlFS): AutoCloseable = {
    new AutoCloseable {
      try {
        // TODO Eventually, we want to have the mount point configurable.
        val mountPoint = Platform.getNativePlatform.getOS match {
          case WINDOWS => """I:\"""
          case _ => "/tmp/mntfs"
        }
        log.info(s"mount($mountPoint)")
        fs.mount(java.nio.file.Paths.get(mountPoint), false, false)
      } catch { case e: Throwable =>
        log.info(e.getMessage)
        fs.umount(); throw e
      }
      override def close(): Unit = fs.umount()
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
