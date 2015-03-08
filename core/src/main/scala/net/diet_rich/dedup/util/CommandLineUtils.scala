package net.diet_rich.dedup.util

import java.io.File

class CommandLineUtils(args: Array[String]) {
  val command :: repoPath :: options = args.toList
  def repositoryDir = new File(repoPath)

  def optional(key: String): Option[String] = options find (_ startsWith s"$key:") map (_ substring key.length + 1)
  def required(key: String): String = optional(key) getOrElse (throw new IllegalArgumentException(s"option '$key' is mandatory"))
  def intOptional(key: String) = optional(key) map (_ toInt)
}
