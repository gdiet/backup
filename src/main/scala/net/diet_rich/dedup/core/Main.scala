package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util.io.using

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
        val threads = intOptional("storeThreads")
        val storeMethod = optional("storeMethod") map StoreMethod.named
        using(Repository(new File(repoPath), readonly = false, storeMethod, threads)) { repository =>
          using(new BackupAlgorithm(repository, threads)) { backupAlgorithm =>
            backupAlgorithm.backup(source, target)
          }
        }

      case _ => println(s"unknown command $command")
    }
  }
}
