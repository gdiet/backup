package net.diet_rich.dedupfs

import java.io.File

import net.diet_rich.common._

object Main extends App with Logging {
  val arguments = new Arguments(args, 2)
  val List(command, repoPath) = arguments.parameters
  command match {
    case "createRepository" => createRepository()
  }

  def createRepository() = {
    val repositoryid = arguments optional "id"
    val hashAlgorithm = arguments optional "hashAlgorithm"
    val storeBlockSize = arguments longOptional "dataFileSize"
    arguments withSettingsChecked {
      Repository.create(new File(repoPath), repositoryid, hashAlgorithm, storeBlockSize)
    }
  }
}
