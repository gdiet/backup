package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util.{Logging, CommandLineUtils}
import net.diet_rich.dedup.util.io._

object Main extends App with Logging {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val commandLineUtils = CommandLineUtils(args)
    import commandLineUtils._
    command match {
      case "create" =>
        Repository create (
          repositoryDir,
          optional("repositoryid"),
          optional("hashAlgorithm"),
          intOptional("storeBlockSize")
        )

      case "backup" =>
        val source = new File(required("source"))
        val target = required("target")
        val parallel = intOptional("parallel")
        val storeMethod = optional("storeMethod") map StoreMethod.named
        val versionComment = optional("versionComment")
        using(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment)) { repository =>
          wrapInShutdownHook(repository) {
            using(new BackupAlgorithm(repository, parallel)) { _ backup (source, target) }
          }
        }

      case "restore" =>
        val source = required("source")
        val target = new File(required("target"))
        require(source != "/", "Cannot restore starting at root")
        require(target.isDirectory, s"Target $target must be a directory")
        require(target.listFiles.isEmpty, s"Target directory $target must be empty")
        using(Repository openReadOnly repositoryDir) { repository =>
          wrapInShutdownHook(repository) {
            using(new RestoreAlgorithm(repository)) { _ restore(source, target) }
          }
        }

      case _ => println(s"unknown command $command")
    }
  }

  def wrapInShutdownHook(closable: AutoCloseable)(task: => Unit) = {
    val hook = sys addShutdownHook {
      log warn "shutdown requested - shutting down dedup system before job is completed"
      closable close()
      log info "shutdown of dedup system complete"
    }
    try task
    finally hook remove()
  }
}
