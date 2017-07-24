package net.diet_rich.dedup.fs

import net.diet_rich.common.Arguments
import ru.serce.jnrfuse.FuseStubFS

object Main extends App {
  private def failAndPrintUsage(failureDetails: String): Int = {
    print(
      s"""|Failed:
          |$failureDetails
          |
          |Usage:
          |<repository path> <mount point> [optional arguments]
          |
          |Optional Arguments:
          |-storeMethod:[store|deflate]
          |""".stripMargin)
    1
  }

  private val arguments = new Arguments(args, 2)
  private val List(repositoryPath, mountPoint) = arguments.parameters
  private val result = arguments.withSettingsChecked(failAndPrintUsage) {
    0
  }
  System.exit(result)
}

class Main extends FuseStubFS {

}
