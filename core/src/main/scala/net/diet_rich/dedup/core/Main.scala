package net.diet_rich.dedup.core

import net.diet_rich.dedup.util.{Logging, CommandLineUtils}
import net.diet_rich.dedup.util.io._

object Main extends App with Logging {
  CommandLineUtils.forArgs(args) { argDetails => import argDetails._
    command match {
      case "create" =>
        checkOptionUse(repositoryid, hashAlgorithm, storeBlockSize)
        Repository create (repositoryDir, repositoryid, hashAlgorithm, storeBlockSize)
        log info s"Repository in $repositoryDir created."

      case "backup" =>
        checkOptionUse(storeMethod, parallel, versionComment, checkPrint, sourceFile, target, reference)
        using(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment)) { repository =>
          wrapInShutdownHook(repository) {
            using(new BackupAlgorithm(repository, checkPrint, parallel)) { _ backup (sourceFile, target, reference) }
          }
        }
        log info s"Backup of $sourceFile in repository $repositoryDir finished."

      case "restore" =>
        checkOptionUse(source, targetFile)
        require(source != "/", "Cannot restore starting at root")
        require(targetFile.isDirectory, s"Target $targetFile must be a directory")
        require(targetFile.listFiles.isEmpty, s"Target directory $targetFile must be empty")
        using(Repository openReadOnly repositoryDir) { repository =>
          wrapInShutdownHook(repository) {
            using(new RestoreAlgorithm(repository)) { _ restore(source, targetFile) }
          }
        }
        log info s"Restore of $source from repository $repositoryDir finished."

      case _ => println(s"unknown command $command")
    }
  }

  def wrapInShutdownHook(closable: AutoCloseable)(task: => Unit) = {
    val hook = sys addShutdownHook {
      log warn "shutdown requested - shutting down dedup system before job is completed"
      closable close()
    }
    try task
    finally hook remove()
  }
}
