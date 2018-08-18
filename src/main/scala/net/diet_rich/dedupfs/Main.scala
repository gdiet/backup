package net.diet_rich.dedupfs

import net.diet_rich.fusefs.{FuseFS, SqlFS}

object Main extends App {
  val fuseFS = FuseFS.mount(new SqlFS)
  try io.StdIn.readLine("[enter] to exit ...")
  finally fuseFS.close()
}
