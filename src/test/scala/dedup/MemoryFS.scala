package dedup

import java.nio.file.Paths

object MemoryFS extends App {
  val fs = new ru.serce.jnrfuse.examples.MemoryFS()
  try fs.mount(Paths.get("/home/georg/temp/mnt"), true, true)
  finally fs.umount()
}
