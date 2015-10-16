package net.diet_rich.dedupfs.ftpserver

import net.diet_rich.common._
import net.diet_rich.dedupfs.StoreMethod

object Main extends App with Logging {
  val arguments = new Arguments(args, 1)
  val List(repoPath) = arguments.parameters
  val ftpPort = arguments intOptional "port" getOrElse 21
  val writable = arguments booleanOptional "writable" getOrElse false
  val storeMethod = arguments optional "storeMethod" map StoreMethod.named getOrElse StoreMethod.STORE
  val versionComment = if (writable) arguments optional "comment" else None
  arguments withSettingsChecked {
    ???
  }
}
