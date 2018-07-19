package net.diet_rich.dedupfs

import net.diet_rich.fusefs.FuseFS

object Main extends App {
  val fuseFS = FuseFS.mount
  try io.StdIn.readLine("[enter] to exit ...")
  finally fuseFS.close()
}
