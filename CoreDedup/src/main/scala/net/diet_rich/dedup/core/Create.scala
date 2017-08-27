package net.diet_rich.dedup.core

import net.diet_rich.common.Arguments
import net.diet_rich.common.io.{RichFile, writeSettingsFile}
import net.diet_rich.dedup.meta.{MetaBackend, MetaBackendFactory}

object Create extends App {
  private val ERROR = 1
  private val OK = 0

  private def defaultMetaBackend = "net.diet_rich.dedup.meta.h2.H2BackendFactory"
  private def defaultHashAlgorithm = "MD5"
  private def defaultDataBlockSize = 64000000L

  private def failAndPrintUsage(failureDetails: String): Int = {
    print(
      s"""|Failed:
          |$failureDetails
          |
          |Usage:
          |<repository path> [optional arguments]
          |
          |Optional Arguments:
          |-metaBackend:<String>   The metadata backend implementation to use,
          |                        default $defaultMetaBackend.
          |-repositoryId:<String>  The identifier of the repository,
          |                        default is a random numeric id.
          |-hashAlgorithm:<String> The hash algorithm to use, e.g. MD5, SHA-1, SHA-256,
          |                        default $defaultHashAlgorithm.
          |-dataBlockSize:<Long>   The data section length in data files,
          |                        default $defaultDataBlockSize.
          |""".stripMargin)
    ERROR
  }

  private val arguments = new Arguments(args, 1)
  private val List(repositoryPath) = arguments.parameters
  private val metaBackendClassName = arguments.optional("-metaBackend").getOrElse(defaultMetaBackend)
  private val repositoryId = arguments.optional("-repositoryId").getOrElse(s"${util.Random.nextLong()}")
  private val hashAlgorithm = arguments.optional("-hashAlgorithm").getOrElse("MD5")
  private val dataBlockSize = arguments.optionalLong("-dataBlockSize").getOrElse(defaultDataBlockSize)

  private val result = arguments.withSettingsChecked(failAndPrintUsage) {
    val metaBackendFactory = Class.forName(metaBackendClassName).newInstance().asInstanceOf[MetaBackendFactory]
    val repositoryDir = new java.io.File(repositoryPath)
    if (!repositoryDir.exists()) repositoryDir.mkdir()
    if (!repositoryDir.isDirectory) failAndPrintUsage("The repository path must be a directory.")
    else if (!repositoryDir.canWrite) failAndPrintUsage("Can not write to repository path.")
    else if (repositoryDir.list().nonEmpty) failAndPrintUsage("The repository directory must be empty.")
    else {
      import net.diet_rich.dedup.Settings._
      val repositorySettings = Map(
        repositoryVersionSetting,
        repositoryIdKey -> repositoryId
      )
      writeSettingsFile(repositoryDir / settingsFileName, repositorySettings)
      metaBackendFactory.initialize(repositoryDir / MetaBackend.path, repositoryId, hashAlgorithm)
      OK
    }
  }
  System.exit(result)
}
