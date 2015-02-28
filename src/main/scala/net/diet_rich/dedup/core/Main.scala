package net.diet_rich.dedup.core

import java.io.File

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val command :: repository :: options = args.toList
    def optional(key: String): Option[String] = options find (_ startsWith s"$key:") map (_ substring key.length+1)
    def required(key: String): String = optional(key) getOrElse (throw new IllegalArgumentException(s"option $key is mandatory"))
    def option(key: String, default: => String): String = optional(key) getOrElse default
    command match {
      case "create" => Repository.create(
        new File(repository),
        optional("repositoryID"),
        optional("hashAlgorithm"),
        optional("storeBlockSize") map (_ toInt)
      )
      case _ => println(s"unknown command $command")
    }
  }
}
