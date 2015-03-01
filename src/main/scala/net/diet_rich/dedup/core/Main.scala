package net.diet_rich.dedup.core

import java.io.File

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util.io.using

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val command :: repoPath :: options = args.toList
    def optional(key: String): Option[String] = options find (_ startsWith s"$key:") map (_ substring key.length+1)
    def required(key: String): String = optional(key) getOrElse (throw new IllegalArgumentException(s"option '$key' is mandatory"))
    def option(key: String, default: => String): String = optional(key) getOrElse default
    command match {

      case "create" => Repository.create(
        new File(repoPath),
        optional("repositoryID"),
        optional("hashAlgorithm"),
        optional("storeBlockSize") map (_ toInt)
      )

      case "backup" =>
        val source = new File(required("source"))
        val target = required("target")
        val threads = optional("storeThreads") map (_ toInt)
        using(Repository(
          new File(repoPath),
          false,
          optional("storeMethod") map StoreMethod.named,
          threads
        )) { repository =>
          using(new BackupAlgorithm(
            repository,
            threads
          )) { backupAlgorithm =>
            backupAlgorithm.backup(source, target)
          }
        }

      case _ => println(s"unknown command $command")
    }
  }
}
