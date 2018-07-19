package net.diet_rich.fusefs

import ru.serce.jnrfuse.FuseStubFS

class FuseFS extends FuseStubFS {

}

object FuseFS {
  def mount: AutoCloseable = {
    new AutoCloseable {
      val fs = new FuseFS
      try {
        val mountPoint = jnr.ffi.Platform.getNativePlatform.getOS match {
          case jnr.ffi.Platform.OS.WINDOWS => "I:\\"
          case _ => "/tmp/mntfs"
        }
        fs.mount(java.nio.file.Paths.get(mountPoint), false, true)
      } catch { case e: Throwable => fs.umount(); throw e }
      override def close(): Unit = fs.umount()
    }
  }
}
