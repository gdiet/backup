package net.diet_rich.dedupfs

import net.diet_rich.fusefs.FuseFS
import net.diet_rich.scalafs.StaticFS

object Main extends App {
  val fuseFS = FuseFS.mount(StaticFS)
  try io.StdIn.readLine("[enter] to exit ...")
  finally fuseFS.close()
}
