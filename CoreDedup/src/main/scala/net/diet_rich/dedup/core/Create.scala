package net.diet_rich.dedup.core

import net.diet_rich.common.Arguments

object Create extends App {
  private def failAndPrintUsage(failureDetails: String): Int = {
    print(
      s"""|Failed:
          |$failureDetails
          |
          |Usage:
          |<repository path> [optional arguments]
          |
          |Optional Arguments:
          |-repositoryId:<String>  The identifier of the repository.
          |-hashAlgorithm:<String> The hash algorithm to use, e.g. MD5, SHA-1, SHA-256.
          |-dataBlockSize:<Long>   The data section length in data files.
          |""".stripMargin)
    1
  }

  private val arguments = new Arguments(args, 1)
  private val List(repositoryPath) = arguments.parameters
  private val repositoryId = arguments.optional("-repositoryId").getOrElse(s"${util.Random.nextLong()}")
  private val hashAlgorithm = arguments.optional("-hashAlgorithm").getOrElse("MD5")
  private val dataFileSize = arguments.optional("-dataBlockSize").getOrElse("64000000")

  private val result = arguments.withSettingsChecked(failAndPrintUsage) {
    val repositoryDir = new java.io.File(repositoryPath)
    if (!repositoryDir.exists()) repositoryDir.mkdir()
    if (!repositoryDir.isDirectory) failAndPrintUsage("The repository path must be a directory.")
    else if (!repositoryDir.canWrite) failAndPrintUsage("Can not write to repository path.")
    else if (repositoryDir.list().nonEmpty) failAndPrintUsage("The repository directory must be empty.")
    else {
      ???
    }
  }
  System.exit(result)
}
