package net.diet_rich.dedup.core

import java.io.{FileOutputStream, File}

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.io._

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val command :: repoPath :: options = args.toList

    def optional(key: String): Option[String] = options find (_ startsWith s"$key:") map (_ substring key.length+1)
    def required(key: String): String = optional(key) getOrElse (throw new IllegalArgumentException(s"option '$key' is mandatory"))
    def intOptional(key: String) = optional(key) map (_ toInt)

    command match {
      case "create" =>
        Repository.create(
          new File(repoPath),
          optional("repositoryid"),
          optional("hashAlgorithm"),
          intOptional("storeBlockSize")
        )

      case "backup" =>
        val source = new File(required("source"))
        val target = required("target")
        val parallel = intOptional("parallel")
        val storeMethod = optional("storeMethod") map StoreMethod.named
        using(Repository readWrite (new File(repoPath), storeMethod, parallel)) { repository =>
          using(new BackupAlgorithm(repository, parallel)) { _ backup (source, target) }
        }

      case "restore" =>
        val source = required("source")
        val target = new File(required("target"))
        require(source != "/", "Cannot restore starting at root")
        require(target.isDirectory, s"Target $target must be a directory")
        require(target.listFiles.isEmpty, s"Target directory $target must be empty")
        using(Repository readOnly new File(repoPath)) { repository =>
          using(new RestoreAlgorithm(repository)) { _ restore(source, target) }
        }

      case _ => println(s"unknown command $command")
    }
  }
}
