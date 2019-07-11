package dedup

import jnr.ffi.Platform.{OS, getNativePlatform}

object Step4_Server extends App {
  Server.run(Map("mount" -> (if (getNativePlatform.getOS == OS.WINDOWS) "J:\\" else "/tmp/mnt")))
}
